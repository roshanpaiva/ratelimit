package redis

import (
	"math"

	pb "github.com/lyft/ratelimit/proto/envoy/service/ratelimit/v2"
	"github.com/lyft/ratelimit/src/assert"
	"github.com/lyft/ratelimit/src/config"
	logger "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func (this *rateLimitCacheImpl) DoTokenLimit(
	ctx context.Context,
	request *pb.RateLimitRequest,
	limits []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {
	// Similar parameters to normal DoLimit; however, RateLimit now has
	// capacity (max num of tokens) field and Descriptor Status has TokenRemaining.

	logger.Debugf("starting cache lookup for token bucket")

	conn := this.pool.Get()
	defer this.pool.Put(conn)

	// Optional connection for per second limits. If the cache has a perSecondPool setup,
	// then use a connection from the pool for per second limits.
	var perSecondConn Connection = nil
	if this.perSecondPool != nil {
		perSecondConn = this.perSecondPool.Get()
		defer this.perSecondPool.Put(perSecondConn)
	}

	// request.HitsAddend could be 0 (default value) if not specified by the caller in the Ratelimit request.
	hitsAddend := max(1, request.HitsAddend)

	// First build a list of all cache keys that we are actually going to hit.
	// generateCacheKey() returns an empty string in the key if there is no limit
	// so that we can keep the arrays all the same size.
	assert.Assert(len(request.Descriptors) == len(limits))
	cacheKeys := make([]cacheKey, len(request.Descriptors))
	now := this.timeSource.UnixNow()
	for i := 0; i < len(request.Descriptors); i++ {
		// TokenBucket cache keys do not have a time divider at the end.
		// Only uses domain key values to define cache key.
		cacheKeys[i] = this.generateCacheKey(request.Domain, request.Descriptors[i], limits[i], now, "tokenBucket")

		// Increase statistics for limits hit by their respective requests.
		if limits[i] != nil {
			limits[i].Stats.TotalHits.Add(uint64(hitsAddend))
		}
	}

	// Now, actually setup the pipeline, skipping empty cache keys.
	for i, cacheKey := range cacheKeys {
		if cacheKey.key == "" {
			continue
		}
		logger.Debugf("looking up cache key: %s", cacheKey.key)

		expirationSeconds := unitToDivider(limits[i].Limit.Unit, limits[i].Limit.Duration)
		if this.expirationJitterMaxSeconds > 0 {
			expirationSeconds += this.jitterRand.Int63n(this.expirationJitterMaxSeconds)
		}

		// Get the previous time and number of tokens from the Redis database.
		// If either is undefined, HMGET returns nil.
		if perSecondConn != nil && cacheKey.perSecond {
			perSecondConn.PipeAppend("HMGET", cacheKey.key, "prevTime", "numTokens")
		} else {
			conn.PipeAppend("HMGET", cacheKey.key, "prevTime", "numTokens")
		}

	}

	// Now fetch the pipeline.
	responseDescriptorStatuses := make([]*pb.RateLimitResponse_DescriptorStatus,
		len(request.Descriptors))

	for i, cacheKey := range cacheKeys {
		// If there is no limit, accept the request.
		if cacheKey.key == "" {
			responseDescriptorStatuses[i] =
				&pb.RateLimitResponse_DescriptorStatus{
					Code:            pb.RateLimitResponse_OK,
					CurrentLimit:    nil,
					TokensRemaining: 0,
				}
			continue
		}

		var limitAfterIncrease float64

		// We record the response of the previous PipeAppends for "prevTime" and "numTokens".
		// Use the perSecondConn if it is not nil and the cacheKey represents a per second Limit.
		var tokenResponse Response
		if this.perSecondPool != nil && cacheKey.perSecond {
			tokenResponse = perSecondConn.PipeResponse()
		} else {
			tokenResponse = conn.PipeResponse()
		}

		// tokensLeft is calculated based off of previous time and refill rate.
		var tokensLeft float64

		ret := tokenResponse.Array()
		timeStampResponse := ret[0]
		currTokensResponse := ret[1]

		// Nil response means there is no redis entry, we create a new entry with max capacity.
		if timeStampResponse == nil || currTokensResponse == nil {
			limitAfterIncrease = limits[i].Limit.TokenBucketCapacity - float64(hitsAddend)
			if limitAfterIncrease >= 0 {
				responseDescriptorStatuses[i] =
					&pb.RateLimitResponse_DescriptorStatus{
						Code:         pb.RateLimitResponse_OK,
						CurrentLimit: limits[i].Limit,
						// Set TokensRemaining for new bucket to be capacity - hitsAddend.
						TokensRemaining: limitAfterIncrease,
					}
			} else {
				// Request is denied, user is still created with max capacity.
				limitAfterIncrease = limits[i].Limit.TokenBucketCapacity
				responseDescriptorStatuses[i] =
					&pb.RateLimitResponse_DescriptorStatus{
						Code:         pb.RateLimitResponse_OVER_LIMIT,
						CurrentLimit: limits[i].Limit,
						// Set TokensRemaining for new bucket to be capacity - hitsAddend
						// Used to be just capacity
						TokensRemaining: limitAfterIncrease,
					}
			}
		} else {
			// Otherwise, use timestamps and ratelimit to calculate refilled tokens, up to max capacity.
			timeStampRaw, _ := timeStampResponse.Int()
			timeStamp := int64(timeStampRaw)
			currTokensRaw, _ := currTokensResponse.Float64()
			currTokens := float64(currTokensRaw)
			refillRatePerSecond := float64(limits[i].Limit.RequestsPerUnit) / float64(unitToDivider(limits[i].Limit.Unit, limits[i].Limit.Duration))
			timeDifference := float64(now - timeStamp)

			// tokensLeft should either be the new amount of tokens or bucket capacity
			tokensLeft = math.Min(currTokens+(timeDifference*refillRatePerSecond), limits[i].Limit.TokenBucketCapacity)

			limitBeforeIncrease := tokensLeft

			overLimitThreshold := float64(0)
			// The nearLimitThreshold is the number of requests that can be made before hitting the NearLimitRatio.
			// We need to know it in both the OK and OVER_LIMIT scenarios.
			nearLimitThreshold := float64((limits[i].Limit.TokenBucketCapacity) * (float64(1) - config.NearLimitRatio))
			limitAfterIncrease = limitBeforeIncrease - float64(hitsAddend)

			logger.Debugf("cache key: %s current: %f", cacheKey.key, limitAfterIncrease)

			// Rejecting the request if not sufficient tokens (no tokens taken)
			if limitAfterIncrease < 0 {
				responseDescriptorStatuses[i] =
					&pb.RateLimitResponse_DescriptorStatus{
						Code:            pb.RateLimitResponse_OVER_LIMIT,
						CurrentLimit:    limits[i].Limit,
						TokensRemaining: limitBeforeIncrease,
					}

				// Increase over limit statistics. Because we support += behavior for increasing the limit, we need to
				// assess if the entire hitsAddend were over the limit. That is, if the limit's value before adding the
				// N hits was over the limit, then all the N hits were over limit.
				// Otherwise, only the difference between the current limit value and the over limit threshold
				// were over limit hits.
				if limitBeforeIncrease <= overLimitThreshold {
					limits[i].Stats.OverLimit.Add(uint64(hitsAddend))
				} else {
					limits[i].Stats.OverLimit.Add(uint64(overLimitThreshold - limitAfterIncrease))

					// If the limit before increase was below the over limit value, then some of the hits were
					// in the near limit range.
					limits[i].Stats.NearLimit.Add(uint64(math.Round(math.Abs(math.Min(nearLimitThreshold, limitBeforeIncrease)))))
				}
				// Request is denied so set limit back to what it was before
				limitAfterIncrease = limitBeforeIncrease
			} else {
				// Here we are updating user and token count
				responseDescriptorStatuses[i] =
					&pb.RateLimitResponse_DescriptorStatus{
						Code:            pb.RateLimitResponse_OK,
						CurrentLimit:    limits[i].Limit,
						TokensRemaining: limitAfterIncrease,
					}

				// The limit is OK but we additionally want to know if we are near the limit.
				if limitAfterIncrease < nearLimitThreshold {
					// Here we also need to assess which portion of the hitsAddend were in the near limit range.
					// If all the hits were over the nearLimitThreshold, then all hits are near limit. Otherwise,
					// only the difference between the current limit value and the near limit threshold were near
					// limit hits.
					if limitBeforeIncrease <= nearLimitThreshold {
						limits[i].Stats.NearLimit.Add(uint64(hitsAddend))
					} else {
						limits[i].Stats.NearLimit.Add(uint64(nearLimitThreshold - limitAfterIncrease))
					}
				}
			}
		}

		// Here we update the redis database entry to reflect our choice in the token bucket algorithm.
		// If cacheKey.key is undefined HMSET creates a new bucket.
		if perSecondConn != nil && cacheKey.perSecond {
			perSecondConn.PipeAppend("HMSET", cacheKey.key, "prevTime", now, "numTokens", limitAfterIncrease)
		} else {
			conn.PipeAppend("HMSET", cacheKey.key, "prevTime", now, "numTokens", limitAfterIncrease)
		}
	}

	// Here we execute each PipeAppend to set the redis database entries
	for _, cacheKey := range cacheKeys {
		if cacheKey.key != "" {
			if perSecondConn != nil && cacheKey.perSecond {
				perSecondConn.PipeResponse()
			} else {
				conn.PipeResponse()
			}
		}
	}
	return responseDescriptorStatuses
}
