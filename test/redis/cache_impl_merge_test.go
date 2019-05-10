package redis_test

import (
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	stats "github.com/lyft/gostats"
	pb "github.com/lyft/ratelimit/proto/envoy/service/ratelimit/v2"
	"github.com/lyft/ratelimit/src/config"
	"github.com/lyft/ratelimit/src/redis"
	"github.com/lyft/ratelimit/test/common"
	mock_redis "github.com/lyft/ratelimit/test/mocks/redis"
	redisSrc "github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisMerge(t *testing.T) {
	t.Run("WithoutPerSecondRedis", testRedisMerge(false))
	t.Run("WithPerSecondRedis", testRedisMerge(true))
}

// Tests that we can process rate limits with different specified algorithms all at once.
func testRedisMerge(usePerSecondRedis bool) func(*testing.T) {
	return func(t *testing.T) {
		assert := assert.New(t)
		controller := gomock.NewController(t)
		defer controller.Finish()

		pool := mock_redis.NewMockPool(controller)
		perSecondPool := mock_redis.NewMockPool(controller)
		timeSource := mock_redis.NewMockTimeSource(controller)
		connection := mock_redis.NewMockConnection(controller)
		perSecondConnection := mock_redis.NewMockConnection(controller)
		response := mock_redis.NewMockResponse(controller)
		var cache redis.RateLimitCache
		if usePerSecondRedis {
			cache = redis.NewRateLimitCacheImpl(pool, perSecondPool, timeSource, rand.New(rand.NewSource(1)), 0)
		} else {
			cache = redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0)
		}
		statsStore := stats.NewStore(stats.NewNullSink(), false)

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)
		pool.EXPECT().Get().Return(connection)
		pool.EXPECT().Get().Return(connection)
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		var connUsed *mock_redis.MockConnection
		if usePerSecondRedis {
			connUsed = perSecondConnection
		} else {
			connUsed = connection
		}

		// FIRST TEST - THREE WAY TESTING
		// Implementing the first three tests of each into a single request

		// Fixed Window Testing
		// Add request and expiration
		// Expect to increment key by 1 because of 1 request
		// Expect to expire the key after 1 seconds because the window will have passed
		connUsed.EXPECT().PipeAppend("INCRBY", "domain_keyF_valueF_1234", uint32(1))
		connUsed.EXPECT().PipeAppend("EXPIRE", "domain_keyF_valueF_1234", int64(1))
		// Update stats for initial request
		connUsed.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(5))
		connUsed.EXPECT().PipeResponse()

		// Sliding Window Testing
		// Add request and expiration
		// Expect to increment key by 1 because of 1 request
		// Expect to expire the key after 2 seconds because the window will have passed
		connUsed.EXPECT().PipeAppend("INCRBY", "domain_keyS_valueS_1234", uint32(1))
		connUsed.EXPECT().PipeAppend("EXPIRE", "domain_keyS_valueS_1234", int64(2))
		// Update stats for initial request
		connUsed.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connUsed.EXPECT().PipeResponse()
		// Get previous subwindows using MGET
		// Window is 1 second so expect to get amount of requests for the past 2 seconds in an array
		connUsed.EXPECT().PipeExec("MGET", []string{"domain_keyS_valueS_1234", "domain_keyS_valueS_1233"}).Return(response)
		response.EXPECT().Array().Return([]*redisSrc.Resp{redisSrc.NewResp(1), redisSrc.NewResp(0)})

		// Token Bucket Testing
		// Except to get the previous time (1233) and number of tokens (10) from redis
		connUsed.EXPECT().PipeAppend("HMGET", "domain_keyT_valueT_", "prevTime", "numTokens")
		// Except the previous time to be one second before now (1233) with max number of tokens
		connUsed.EXPECT().PipeResponse().Return(response)
		resps1 := make([]*redisSrc.Resp, 2)
		resps1[0] = redisSrc.NewResp(int64(1233))
		resps1[1] = redisSrc.NewResp(float64(10))
		response.EXPECT().Array().Return(resps1)
		// Set the updated token number such that request is approved at time 1234 and one token is removed
		connUsed.EXPECT().PipeAppend("HMSET", "domain_keyT_valueT_", "prevTime", int64(1234), "numTokens", float64(9))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
			perSecondPool.EXPECT().Put(perSecondConnection)
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)
		pool.EXPECT().Put(connection)
		pool.EXPECT().Put(connection)

		// Create a request that uses all three key-value pairs
		request := common.NewRateLimitRequest("domain", [][][2]string{{{"keyF", "valueF"}}, {{"keyS", "valueS"}}, {{"keyT", "valueT"}}}, 1)
		// Create three rate limits-
		// 1) unit: limit: 10, second, and algType: fixedWindow
		// 2) unit: limit: 10, second, and algType: slidingWindow
		// 3) refillRate: 10, unit: second, tokenBucketCapacity : 10, and algType: tokenBucket
		limits := []*config.RateLimit{config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_SECOND, 1, "fixedWindow", "keyF_valueF", statsStore),
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_SECOND, 1, "slidingWindow", "keyS_valueS", statsStore),
			config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "keyT_valueT", statsStore, 10)}

		// Assert that each request was allowed and there is 5 limits remaining for
		// fixed Window, 9 limits remaining for sliding window, and 9 tokens remaining for tokenBucket
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 5},
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[1].Limit, LimitRemaining: 9},
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[2].Limit, TokensRemaining: 9.0}},
			cache.DoLimit(nil, request, limits))
		// Check the stats are accurate
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
		assert.Equal(uint64(1), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[1].Stats.NearLimit.Value())
		assert.Equal(uint64(1), limits[2].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[2].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[2].Stats.NearLimit.Value())
	}
}
