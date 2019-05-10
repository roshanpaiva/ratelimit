package redis

import (
	"bytes"
	"math/rand"
	"strconv"

	//"strconv"
	"sync"
	"time"

	pb_struct "github.com/lyft/ratelimit/proto/envoy/api/v2/ratelimit"
	ratelimit "github.com/lyft/ratelimit/proto/envoy/api/v2/ratelimit"
	pb "github.com/lyft/ratelimit/proto/envoy/service/ratelimit/v2"
	"github.com/lyft/ratelimit/src/config"
	logger "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type rateLimitCacheImpl struct {
	pool Pool
	// Optional Pool for a dedicated cache of per second limits.
	// If this pool is nil, then the Cache will use the pool for all
	// limits regardless of unit. If this pool is not nil, then it
	// is used for limits that have a SECOND unit.
	perSecondPool              Pool
	timeSource                 TimeSource
	jitterRand                 *rand.Rand
	expirationJitterMaxSeconds int64
	// bytes.Buffer pool used to efficiently generate cache keys.
	bufferPool sync.Pool
}

// Convert a rate limit into a time divider.
// @param unit supplies the unit to convert.
// @return the divider to use in time computations.
func unitToDivider(unit pb.RateLimitResponse_RateLimit_Unit, duration uint32) int64 {
	if (duration == 0) {
		duration = 1
	}
	switch unit {
	case pb.RateLimitResponse_RateLimit_SECOND:
		return 1 * int64(duration)
	case pb.RateLimitResponse_RateLimit_MINUTE:
		return 60 * int64(duration)
	case pb.RateLimitResponse_RateLimit_HOUR:
		return 60 * 60 * int64(duration)
	case pb.RateLimitResponse_RateLimit_DAY:
		return 60 * 60 * 24 * int64(duration)
	}

	panic("should not get here")
}

// Convert a rate limit into a time sub-divider for sliding window
// @param unit supplies the unit to convert.
// @return the subdivider to use in time computations, where the smallest subwindow size will be 1 second.
func unitToSubDivider(unit pb.RateLimitResponse_RateLimit_Unit, duration uint32) int64 {
	switch unit {
	case pb.RateLimitResponse_RateLimit_SECOND:
		return 1
	case pb.RateLimitResponse_RateLimit_MINUTE:
		return 1 * int64(duration)
	case pb.RateLimitResponse_RateLimit_HOUR:
		return 60 * int64(duration)
	case pb.RateLimitResponse_RateLimit_DAY:
		return 60 * 60 * int64(duration)
	}

	panic("should not get here")
}

// Generate a cache key for a limit lookup.
// @param domain supplies the cache key domain.
// @param descriptor supplies the descriptor to generate the key for.
// @param limit supplies the rate limit to generate the key for (may be nil).
// @param now supplies the current unix time.
// @param algType suppplies the type of algorithm the cache key will be used for
// @return cacheKey struct for a fixed window.
func (this *rateLimitCacheImpl) generateCacheKey(
	domain string, descriptor *pb_struct.RateLimitDescriptor, limit *config.RateLimit, now int64, algType string) cacheKey {
	if algType == "" {
		algType = "fixedWindow"
	}

	if limit == nil {
		return cacheKey{
			key:       "",
			perSecond: false,
		}
	}

	b := this.bufferPool.Get().(*bytes.Buffer)
	defer this.bufferPool.Put(b)
	b.Reset()

	b.WriteString(domain)
	b.WriteByte('_')

	for _, entry := range descriptor.Entries {
		b.WriteString(entry.Key)
		b.WriteByte('_')
		b.WriteString(entry.Value)
		b.WriteByte('_')
	}

	switch algType {
	case "slidingWindow":
		// Each key in sliding window algorithm is based on a time sub-divider that is smaller than actual rate limit
		subDivider := unitToSubDivider(limit.Limit.Unit, limit.Limit.Duration)
		b.WriteString(strconv.FormatInt((now/subDivider)*subDivider, 10))
	case "tokenBucket":
		break
	case "fixedWindow":
		// Each key in fixed window algorithm is based on a time divider
		fallthrough
	default:
		// If algorithm type is not specified, construct cache key as fixed window
		divider := unitToDivider(limit.Limit.Unit, limit.Limit.Duration)
		b.WriteString(strconv.FormatInt((now/divider)*divider, 10))

	}

	return cacheKey{
		key:       b.String(),
		perSecond: isPerSecondLimit(limit.Limit.Unit)}
}

func isPerSecondLimit(unit pb.RateLimitResponse_RateLimit_Unit) bool {
	return unit == pb.RateLimitResponse_RateLimit_SECOND
}

func max(a uint32, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

type cacheKey struct {
	key string
	// True if the key corresponds to a limit with a SECOND unit. False otherwise.
	perSecond bool
}

func pipelineAppend(conn Connection, key string, hitsAddend uint32, expirationSeconds int64) {
	conn.PipeAppend("INCRBY", key, hitsAddend)
	conn.PipeAppend("EXPIRE", key, expirationSeconds)
}

func pipelineFetch(conn Connection) uint32 {
	ret := uint32(conn.PipeResponse().Int())
	// Pop off EXPIRE response and check for error.
	conn.PipeResponse()
	return ret
}

func (this *rateLimitCacheImpl) DoLimit(
	ctx context.Context,
	request *pb.RateLimitRequest,
	limits []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {

	fixedWindowLimits := make([]*config.RateLimit, 0)
	slidingWindowLimits := make([]*config.RateLimit, 0)
	tokenBucketLimits := make([]*config.RateLimit, 0)

	fixedWindowDescriptors := make([]*ratelimit.RateLimitDescriptor, 0)
	slidingWindowDescriptors := make([]*ratelimit.RateLimitDescriptor, 0)
	tokenBucketDescriptors := make([]*ratelimit.RateLimitDescriptor, 0)

	// Separate limits into new arrays based on algorithm descriptor
	logger.Debugf("sorting requests by algorithm")
	for i := 0; i < len(limits); i++ {
		if limits[i] != nil {
			switch limits[i].Limit.AlgType {
			case "slidingWindow":
				slidingWindowLimits = append(slidingWindowLimits, limits[i])
				slidingWindowDescriptors = append(slidingWindowDescriptors, request.Descriptors[i])
			case "tokenBucket":
				tokenBucketLimits = append(tokenBucketLimits, limits[i])
				tokenBucketDescriptors = append(tokenBucketDescriptors, request.Descriptors[i])
			case "fixedWindow":
				fallthrough
			default:
				fixedWindowLimits = append(fixedWindowLimits, limits[i])
				fixedWindowDescriptors = append(fixedWindowDescriptors, request.Descriptors[i])
			}
		}
	}

	fixedWindowRequest := pb.RateLimitRequest{
		Domain:     request.Domain,
		HitsAddend: request.HitsAddend,
		Descriptors: fixedWindowDescriptors,
	}
	slidingWindowRequest := pb.RateLimitRequest{
		Domain:     request.Domain,
		HitsAddend: request.HitsAddend,
		Descriptors: slidingWindowDescriptors,
	}
	tokenBucketRequest := pb.RateLimitRequest{
		Domain:     request.Domain,
		HitsAddend: request.HitsAddend,
		Descriptors: tokenBucketDescriptors,
	}

	// Use each of the newly created requests and limits to run the algorithm.
	var fixedWindowResponse []*pb.RateLimitResponse_DescriptorStatus
	var slidingWindowResponse []*pb.RateLimitResponse_DescriptorStatus
	var tokenBucketResponse []*pb.RateLimitResponse_DescriptorStatus
	if len(fixedWindowLimits) > 0 {
		fixedWindowResponse = this.DoFixedWindowLimit(ctx, &fixedWindowRequest, fixedWindowLimits)
	}
	if len(slidingWindowLimits) > 0 {
		slidingWindowResponse = this.DoSlidingWindowLimit(ctx, &slidingWindowRequest, slidingWindowLimits)
	}
	if len(tokenBucketLimits) > 0 {
		tokenBucketResponse = this.DoTokenLimit(ctx, &tokenBucketRequest, tokenBucketLimits)
	}

	var responses []*pb.RateLimitResponse_DescriptorStatus
	var resp *pb.RateLimitResponse_DescriptorStatus

	logger.Debugf("sorting responses back into original order")
	for i := 0; i < len(limits); i++ {
		if limits[i] != nil {
			switch limits[i].Limit.AlgType {
			case "slidingWindow":
				resp, slidingWindowResponse = slidingWindowResponse[0], slidingWindowResponse[1:]
			case "tokenBucket":
				resp, tokenBucketResponse = tokenBucketResponse[0], tokenBucketResponse[1:]
			case "fixedWindow":
				fallthrough
			default:
				resp, fixedWindowResponse = fixedWindowResponse[0], fixedWindowResponse[1:]
			}

		} else {
			resp = &pb.RateLimitResponse_DescriptorStatus{
				Code:           pb.RateLimitResponse_OK,
				CurrentLimit:   nil,
				LimitRemaining: 0,
			}
		}
		responses = append(responses, resp)
	}

	return responses
}

func NewRateLimitCacheImpl(pool Pool, perSecondPool Pool, timeSource TimeSource, jitterRand *rand.Rand, expirationJitterMaxSeconds int64) RateLimitCache {
	return &rateLimitCacheImpl{
		pool:                       pool,
		perSecondPool:              perSecondPool,
		timeSource:                 timeSource,
		jitterRand:                 jitterRand,
		expirationJitterMaxSeconds: expirationJitterMaxSeconds,
		bufferPool:                 newBufferPool(),
	}
}

func newBufferPool() sync.Pool {
	return sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
}

type timeSourceImpl struct{}

func NewTimeSourceImpl() TimeSource {
	return &timeSourceImpl{}
}

func (this *timeSourceImpl) UnixNow() int64 {
	return time.Now().Unix()
}

// rand for jitter.
type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func NewLockedSource(seed int64) JitterRandSource {
	return &lockedSource{src: rand.NewSource(seed)}
}

func (r *lockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}
