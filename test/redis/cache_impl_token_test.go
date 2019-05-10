package redis_test

import (
	"math/rand"
	"testing"

	stats "github.com/lyft/gostats"
	pb "github.com/lyft/ratelimit/proto/envoy/service/ratelimit/v2"
	"github.com/lyft/ratelimit/src/config"
	"github.com/lyft/ratelimit/src/redis"
	redisSrc "github.com/mediocregopher/radix.v2/redis"

	"github.com/golang/mock/gomock"
	"github.com/lyft/ratelimit/test/common"
	mock_redis "github.com/lyft/ratelimit/test/mocks/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisToken(t *testing.T) {
	t.Run("WithoutPerSecondRedis", testRedisToken(false))
	t.Run("WithPerSecondRedis", testRedisToken(true))
	t.Run("Consecutive", TestConsecutiveCapacityCalls)
}

func testRedisToken(usePerSecondRedis bool) func(*testing.T) {
	return func(t *testing.T) {

		/* FIRST TEST - FULL BUCKET
		   This test tests for a request made when a token bucket is full.
		   In this case, the algorithm first attempts to refill the bucket before
		   removing tokens that the request requires.
		   Since the token bucket is full with 10 tokens, no new tokens are added,
		   and one is taken away by the request, resulting in 9 tokens.
		*/

		assert := assert.New(t)
		controller := gomock.NewController(t)
		defer controller.Finish()

		/*
			Mock all the objects that aren't directly related to the algorithm logic,
			such as the connection pool, time, connections, and connection response
		*/
		pool := mock_redis.NewMockPool(controller)
		perSecondPool := mock_redis.NewMockPool(controller)
		timeSource := mock_redis.NewMockTimeSource(controller)
		connection := mock_redis.NewMockConnection(controller)
		perSecondConnection := mock_redis.NewMockConnection(controller)
		response := mock_redis.NewMockResponse(controller)
		var cache redis.RateLimitCache
		/*
			If the limit is in terms of seconds, we want to use a separate, dedicated
			connection to access the Redis DB
		*/
		if usePerSecondRedis {
			cache = redis.NewRateLimitCacheImpl(pool, perSecondPool, timeSource, rand.New(rand.NewSource(1)), 0)
		} else {
			cache = redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0)
		}
		statsStore := stats.NewStore(stats.NewNullSink(), false)

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the current time as 1234 seconds
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		var connUsed *mock_redis.MockConnection
		if usePerSecondRedis {
			connUsed = perSecondConnection
		} else {
			connUsed = connection
		}
		// Get entries from the Redis DB for time of last request (prevTime) and
		// tokens in the bucket (numTokens)
		connUsed.EXPECT().PipeAppend("HMGET", "domain_key_value_", "prevTime", "numTokens")

		// Mock the reponses we get
		connUsed.EXPECT().PipeResponse().Return(response)

		// Mock the time of last request as 1233 seconds
		resps := make([]*redisSrc.Resp, 2)
		resps[0] = redisSrc.NewResp(int64(1233))
		resps[1] = redisSrc.NewResp(float64(10))
		// Mock the number of tokens in the bucket as 10. 1 second has passed,
		// so we attempt to add 1 token.
		response.EXPECT().Array().Return(resps)

		// Expect the database entry to be updated with time of 1234 (current time)
		// and 9 tokens after the request has been called
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key_value_", "prevTime", int64(1234), "numTokens", float64(9))
		connUsed.EXPECT().PipeResponse()
		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with hitsAddend/weight of 1
		request := common.NewRateLimitRequest("domain", [][][2]string{{{"key", "value"}}}, 1)
		// Initialize a bucket with capacity of 10 and refill rate of 1
		limits := []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key_value", statsStore, 10)}

		// Expect the number of tokens after the request has been made to be 9
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, TokensRemaining: 9.0}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

		/* SECOND TEST - OVER LIMIT AND NIL LIMIT
		   This test tests two things: it first tests what happens when we are at
		   the limit, meaning when the token bucket is empty and we still try to
		   remove tokens. It also tests what happens when an inputted limit is nil.
		   In this test, the first descriptors's limit is nil, so it should default
		   to accepting any request that comes in.

		   Additionally, we initialize the bucket to have 0 tokens, and the second
		   request tries to take a token away. The result should be an OVER_LIMIT
		   response for the second limit.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the current time as 1234 seconds
		timeSource.EXPECT().UnixNow().Return(int64(1234))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key2_value2_subkey2_subvalue2_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)

		// Mock the time of last request as 1234 seconds
		resps2 := make([]*redisSrc.Resp, 2)
		resps2[0] = redisSrc.NewResp(int64(1234))
		resps2[1] = redisSrc.NewResp(float64(0))
		// Mock the number of tokens in the bucket as 0. No time has passed,
		// so 0 tokens added.
		response.EXPECT().Array().Return(resps2)

		// Expect the database entry to be updated with time of 1234 (current time)
		// and 0 tokens after the request has been called and denied.
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key2_value2_subkey2_subvalue2_", "prevTime", int64(1234), "numTokens", float64(0))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with two descriptors with a hitsAddend/weight of 1
		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key2", "value2"}},
				{{"key2", "value2"}, {"subkey2", "subvalue2"}},
			}, 1)

		// Initialize a bucket with capacity of 10 and refill rate of 1
		limits = []*config.RateLimit{nil, config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key2_value2_subkey2_subvalue2", statsStore, 10)}

		// Expect the number of tokens after the request has been made to be 0. Expect the first request (with nil limit) to be successful, and the second request to be unsuccessful
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: nil, TokensRemaining: 0},
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[1].Limit, TokensRemaining: 0.0}},
			cache.DoLimit(nil, request, limits))

		assert.Equal(uint64(1), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[1].Stats.NearLimit.Value())

		/* THIRD TEST - NEAR LIMIT
		   This tests the scenario in which the token bucket is near limit,
		   meaning that there are fewer than 20% (or whatever percentage is
		   specified for the near limit range) of tokens left.
		   In this case, the near limit counter should be incremented to indicate
		   that there are near limit hits.
		   In this specific test, we initialize a token bucket with 1 token, and
		   call a request that removes one token.
		   Since the capacity is 10, as long as there are two or fewer tokens in the
		   bucket, we are near limit. Any removal of any of these two tokens results in a near limit hit.
		   Since we remove 1 token, that 1 token removed is a near limit hit, and
		   the request is successful (with 0 tokens left).
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the current time as 1234 seconds
		timeSource.EXPECT().UnixNow().Return(int64(1234))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key1_value1_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// Mock the time of last request as 1233 seconds
		resps3 := make([]*redisSrc.Resp, 2)
		resps3[0] = redisSrc.NewResp(int64(1234))
		resps3[1] = redisSrc.NewResp(float64(1))
		// Mock the number of tokens in the bucket as 1
		response.EXPECT().Array().Return(resps3)

		// Expect the database entry to be updated with time of 1234 (current time)
		// and 0 tokens after the request has been called
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key1_value1_", "prevTime", int64(1234), "numTokens", float64(0))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with one descriptor with a hitsAddend/weight of 1
		request = common.NewRateLimitRequest("domain", [][][2]string{{{"key1", "value1"}}}, 1)

		// Initialize a bucket with capacity of 10 and refill rate of 1
		limits = []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key1_value1", statsStore, 10)}

		// Expect the number of tokens after the request has been made to be 0.
		// The request is successful as it takes the last token in the bucket.
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, TokensRemaining: 0.0}},
			cache.DoLimit(nil, request, limits))

		// Near limit is incremented by 1 and total hits is as well.
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

		/* FOURTH TEST - WEIGHTED REQUESTS
		   This tests requests that have a hitsAddend/weight greater than 1
		   This is configured as a the final parameter in ommon.NewRateLimitRequest
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the current time as 1234 seconds
		timeSource.EXPECT().UnixNow().Return(int64(1234))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key3_value3_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// Mock the time of last request as 1232 seconds. 2 seconds have passed,
		// so 2 tokens are added
		resps4 := make([]*redisSrc.Resp, 2)
		resps4[0] = redisSrc.NewResp(int64(1232))
		resps4[1] = redisSrc.NewResp(float64(4))
		// Mock the number of tokens in the bucket as 4
		response.EXPECT().Array().Return(resps4)

		/* Expect the database entry to be updated with time of 1234 (current time)
		   and 2 tokens after the request has been called
		   We have 4 tokens in the beginning, two seconds have passed since the last
		   request, so we refill 2 tokens, and the request takes away 4.
		*/
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key3_value3_", "prevTime", int64(1234), "numTokens", float64(2))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with one descriptor with a hitsAddend/weight of 4
		request = common.NewRateLimitRequest("domain", [][][2]string{{{"key3", "value3"}}}, 4)

		// Initialize a bucket with capacity of 10 and refill rate of 1
		limits = []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key3_value3", statsStore, 10)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, TokensRemaining: 2.0}},
			cache.DoLimit(nil, request, limits))

		// Total hits is incremented by 4 since the request weight is 4.
		assert.Equal(uint64(4), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

		/* FIFTH TEST - COMBO TEST
		   Inspiration from TestNearLimit
		   This tests both over limit and near limit behavior, meaning that when
		   there aren't enough tokens in the bucket to process the request, we
			 increment over limit counts as well as near limit counts.
		   In this specific test, the request has a weight of 7, and there are only
		   5 tokens in the bucket. Since the capacity of the bucket is 20, when the
			 request tries to take away 7 tokens, 2 will be over limit, and 4 will be
			 near limt.
		   This is because 20% of 20 is 4 tokens, so the near limit range is 0 to 4
		   tokens. When a request fails, we increment near limit count by the number
		 	 of near limit tokens left in the bucket (0 to 4).
		   We also increment over limit count by the number of tokens needed in
			 addition to the number of tokens already in the bucket to satisfy the request.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		timeSource.EXPECT().UnixNow().Return(int64(1234))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key5_value5_", "prevTime", "numTokens")
		connUsed.EXPECT().PipeResponse().Return(response)
		// No time has passed, so don't add more tokens
		resps5 := make([]*redisSrc.Resp, 2)
		resps5[0] = redisSrc.NewResp(int64(1234))
		resps5[1] = redisSrc.NewResp(float64(5))
		// Mock the number of tokens in the bucket as 5
		response.EXPECT().Array().Return(resps5)

		/* Expect the database entry to be updated with time of 1234 (current time)
			and 5 tokens after the request has been called and rejected.
			Since the request is rejected, we don't take away any tokens, so we are
		    left with the same amount of tokens as before.
		*/
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key5_value5_", "prevTime", int64(1234), "numTokens", float64(5))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with one descriptor with a hitsAddend/weight of 7
		request = common.NewRateLimitRequest("domain", [][][2]string{{{"key5", "value5"}}}, 7)

		// Initialize a bucket with capacity of 20 (near limit range is 0 to 4 tokens)
		// and refill rate of 1
		limits = []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key5_value5", statsStore, 20)}

		// Not enough tokens to satisfy the request, so return OVER_LIMIT response,
		//and keep same number of tokens.
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, TokensRemaining: 5.0}},
			cache.DoLimit(nil, request, limits))

		// Increase number of total hits by weight of request, number of over limit
		// hits by number of tokens in bucket - weight of request, and number of near
		// limit hits by min(number of tokens, 20% of bucket capacity)
		assert.Equal(uint64(7), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(2), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(4), limits[0].Stats.NearLimit.Value())

		/* SIXTH TEST- PARTIAL BUCKET
		   This is a test that makes sure that when we wait a long period of time,
		   when we refill a bucket, it never goes over the bucket capacity.
		   In this specific test, we have 4 tokens in the bucket, but waited 10
		   seconds since the last request, so we attempt to refill 10 tokens.
		   However, 10+4 > 10 (max capacity), so we actually only refill with 6
		   tokens to get max capacity of 10, before processing the request.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		timeSource.EXPECT().UnixNow().Return(int64(1010))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key6_value6_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// 10 seconds have passed, so we refill with as many as 10 tokens.
		resps6 := make([]*redisSrc.Resp, 2)
		resps6[0] = redisSrc.NewResp(int64(1000))
		resps6[1] = redisSrc.NewResp(float64(4))
		// Mock the number of tokens in the bucket as 4
		response.EXPECT().Array().Return(resps6)

		// Expect the database entry to be updated with time of 1010 (current time)
		// and 9 tokens after the request has been called.
		// We expect 9 tokens, as we refill with 6 tokens to get the max capacity of
		// 10 tokens, and remove 1 token since the request takes 1 token away.
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key6_value6_", "prevTime", int64(1010), "numTokens", float64(9))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with a hitsAddend/weight of 1
		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key6", "value6"}},
			}, 1)

		// Initialize a bucket with capacity of 10  and refill rate of 1
		limits = []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key6_value6", statsStore, 10)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, TokensRemaining: 9.0}},
			cache.DoLimit(nil, request, limits))
		// Request has weight of 1, so total hits increases by 1.
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

		/* SEVENTH TEST - MULTIPLE BUCKETS TEST
			   Inspiration from TestNearLimit
			   This is a test that makes sure that if there are multiple buckets, that
			   requests for one bucket do not effect other buckets to maintain the
			   property of independence.
			   In this specific test, we have 2 user's buckets with the following attributes:
		       User 1: number of tokens = 8, refill rate = 2 per second, hit addends = 1, bucket capacity = 50
			   User 2: number of tokens = 6, refill rate = 1 per second, hit addends = 2
			   We expect 27 tokens in user 1's bucket (8 + (2*10) - 1) and we expect 14
			   tokens in user 2's bucket (6 + (1*10) - 2)
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		timeSource.EXPECT().UnixNow().Return(int64(1010))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key71_value71_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// 10 seconds have passed, as the previous time was 1000 and the current is 1010
		resps71 := make([]*redisSrc.Resp, 2)
		resps71[0] = redisSrc.NewResp(int64(1000))
		resps71[1] = redisSrc.NewResp(float64(8))
		// Mock the number of tokens in user 1's bucket as 8
		response.EXPECT().Array().Return(resps71)

		// Expect the database entry to be updated with time of 1010 (current time)
		// and 27 tokens after the request has been called.
		// We expect 27 tokens, as we refill with 20 tokens which is below the max
		// capacity of 50 tokens, and remove 1 token since the request takes 1 token away.
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key71_value71_", "prevTime", int64(1010), "numTokens", float64(27))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with a hitsAddend/weight of 1
		request1 := common.NewRateLimitRequest("domain", [][][2]string{{{"key71", "value71"}}}, 1)
		// Initialize a bucket with capacity of 50  and refill rate of 2
		limits1 := []*config.RateLimit{config.NewRateLimit(2, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key71_value71", statsStore, 50)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits1[0].Limit, TokensRemaining: 27.0}},
			cache.DoLimit(nil, request1, limits1))
		// Request has weight of 1, so total hits increases by 1.
		assert.Equal(uint64(1), limits1[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits1[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits1[0].Stats.NearLimit.Value())

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)
		// This begins the test of user 2's bucket
		timeSource.EXPECT().UnixNow().Return(int64(1010))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key72_value72_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// 10 seconds have passed, as the previous time was 1000 and the current is 1010
		resps72 := make([]*redisSrc.Resp, 2)
		resps72[0] = redisSrc.NewResp(int64(1000))
		resps72[1] = redisSrc.NewResp(float64(6))
		// Mock the number of tokens in user 1's bucket as 6
		response.EXPECT().Array().Return(resps72)

		// Expect the database entry to be updated with time of 1010 (current time)
		// and 14 tokens after the request has been called.
		// We expect 14 tokens, as we refill with 10 tokens which is below the max
		// capacity of 20 tokens, and remove 2 tokens since the request takes 2 tokens away.
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key72_value72_", "prevTime", int64(1010), "numTokens", float64(14))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with a hitsAddend/weight of 2
		request2 := common.NewRateLimitRequest("domain", [][][2]string{{{"key72", "value72"}}}, 2)
		// Initialize a bucket with capacity of 20  and refill rate of 1
		limits2 := []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key72_value72", statsStore, 20)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits2[0].Limit, TokensRemaining: 14.0}},
			cache.DoLimit(nil, request2, limits2))
		// Request has weight of 2, so total hits increases by 2.
		assert.Equal(uint64(2), limits2[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits2[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits2[0].Stats.NearLimit.Value())
		// First request should be isolated from second request, so TotalHits should remain 1.
		assert.Equal(uint64(1), limits1[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits1[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits1[0].Stats.NearLimit.Value())

		/* EIGHTH TEST - DURATION TEST
		   This is a test that tests the duration field for the RateLimit. We set the duration as 2, which means which our refill occurs every 2 units of time.
		   Our unit is in seconds, and our refill rate is 1, so we essentially refill 1 token every 2 seconds.
		   We set the number of tokens in the bucket to initially be 5, and set the duration of time from the previous request to the current request to be 8 seconds.
		   This means that 8/2 tokens are added during the refill, meaning there are 5+(8/2) = 9 tokens total before the request takes away a token, resulting in 8 tokens in the end.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		timeSource.EXPECT().UnixNow().Return(int64(1008))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key8_value8_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// 10 seconds have passed, as the previous time was 1000 and the current is 1008
		resps8 := make([]*redisSrc.Resp, 2)
		resps8[0] = redisSrc.NewResp(int64(1000))
		resps8[1] = redisSrc.NewResp(float64(5))
		// Mock the number of tokens in user 1's bucket as 5
		response.EXPECT().Array().Return(resps8)

		// Expect the database entry to be updated with time of 1008 (current time)
		// and 8 tokens after the request has been called.
		// We expect 8 tokens, as we refill with 4 tokens which is below the max
		// capacity of 10 tokens, and remove 1 token since the request takes 1 token away.
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key8_value8_", "prevTime", int64(1008), "numTokens", float64(8))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with a hitsAddend/weight of 1
		request_test_8 := common.NewRateLimitRequest("domain", [][][2]string{{{"key8", "value8"}}}, 1)
		// Initialize a bucket with capacity of 10  and refill rate of 1 token every 2 seconds
		limits_test_8 := []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 2, "tokenBucket", "key8_value8_", statsStore, 10)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits_test_8[0].Limit, TokensRemaining: 8.0}},
			cache.DoLimit(nil, request_test_8, limits_test_8))
		// Request has weight of 1, so total hits increases by 1.
		assert.Equal(uint64(1), limits_test_8[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits_test_8[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits_test_8[0].Stats.NearLimit.Value())

		// NINTH TEST - DURATION TEST
		// This is a test that tests the duration field for the RateLimit.
		// We set the duration as 5, which means which our refill rate is each every 5 units

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		timeSource.EXPECT().UnixNow().Return(int64(1020))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key9_value9_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// 10 seconds have passed, as the previous time was 1000 and the current is 1020
		resps9 := make([]*redisSrc.Resp, 2)
		resps9[0] = redisSrc.NewResp(int64(1000))
		resps9[1] = redisSrc.NewResp(float64(12))
		// Mock the number of tokens in user 1's bucket as 12
		response.EXPECT().Array().Return(resps9)

		// Expect the database entry to be updated with time of 1020 (current time)
		// and 14 tokens after the request has been called.
		// We expect 14 tokens, as we refill with 4 tokens which is below the max
		// capacity of 20 tokens, and remove 2 token since the request takes 2 tokens away.
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key9_value9_", "prevTime", int64(1020), "numTokens", float64(14))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with a hitsAddend/weight of 2
		request_test_9 := common.NewRateLimitRequest("domain", [][][2]string{{{"key9", "value9"}}}, 2)
		// Initialize a bucket with capacity of 20  and refill rate of 1
		limits_test_9 := []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 5, "tokenBucket", "key9_value9_", statsStore, 20)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits_test_9[0].Limit, TokensRemaining: 14.0}},
			cache.DoLimit(nil, request_test_9, limits_test_9))
		// Request has weight of 1, so total hits increases by 1.
		assert.Equal(uint64(2), limits_test_9[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits_test_9[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits_test_9[0].Stats.NearLimit.Value())


		/* TENTH TEST - NEW USER TEST
		   This tests to make sure that when a user is not present in the database,
			 a new entry is created. We simulate a new user by mocking the response
			 from the Redis database as nil.
			 When this happens, we simply initialize a full bucket for the user, and
			 attempt to subtract the number of tokens from the full bucket equal to
			 the weight of the request.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the current time as 1234 seconds
		timeSource.EXPECT().UnixNow().Return(int64(1234))

		connUsed.EXPECT().PipeAppend("HMGET", "domain_key10_value10_", "prevTime", "numTokens")

		connUsed.EXPECT().PipeResponse().Return(response)
		// Mock the Redis DB response as nil, meaning that the user doesn't exist
		// in the database.
		resps10 := make([]*redisSrc.Resp, 2)
		resps10[0] = redisSrc.NewResp(nil)
		resps10[1] = redisSrc.NewResp(nil)
		response.EXPECT().Array().Return(resps10)

		/* Expect the database entry to be updated with time of 1234 (current time)
		   and 6 tokens after the request has been called.
		   We have 10 tokens in the beginning, since we initialize a full bucket
			 for the new user.
		   The request takes away 4, so we are left with 6 tokens.
		*/
		connUsed.EXPECT().PipeAppend("HMSET", "domain_key10_value10_", "prevTime", int64(1234), "numTokens", float64(6))
		connUsed.EXPECT().PipeResponse()

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		// Make a request with one descriptor with a hitsAddend/weight of 4
		request_test_10 := common.NewRateLimitRequest("domain", [][][2]string{{{"key10", "value10"}}}, 4)

		// Initialize a bucket with capacity of 10 and refill rate of 1
		limits_test_10 := []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key10_value10", statsStore, 10)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits_test_10[0].Limit, TokensRemaining: 6.0}},
			cache.DoLimit(nil, request_test_10, limits_test_10))

		// Total hits is incremented by 4 since the request weight is 4.
		assert.Equal(uint64(4), limits_test_10[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits_test_10[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits_test_10[0].Stats.NearLimit.Value())
	}
}

func TestConsecutiveCapacityCalls(t *testing.T) {
	/* ELEVENTH TEST - MULTIPLE OVERLIMIT TEST
	This test tests for multiple full capacity calls, each at 1 second before full refill
	First call at 9 second with weight 10(capacity) should pass because of max capacity initialization
	Second call at 18s with weight 10 should fail as we should have only 9 tokens at this time
	Third call at 27s with weight 27 should pass as we now have 10 tokens at this time(we dont fill tokens over capacity)
	*/
	assert := assert.New(t)
	controller := gomock.NewController(t)
	defer controller.Finish()

	pool := mock_redis.NewMockPool(controller)
	timeSource := mock_redis.NewMockTimeSource(controller)
	connection := mock_redis.NewMockConnection(controller)
	response := mock_redis.NewMockResponse(controller)
	statsStore := stats.NewStore(stats.NewNullSink(), false)
	// take 10 off at 9 second, passes
	pool.EXPECT().Get().Return(connection)

	timeSource.EXPECT().UnixNow().Return(int64(1234))

	connection.EXPECT().PipeAppend("HMGET", "domain_key3_value3_", "prevTime", "numTokens")

	connection.EXPECT().PipeResponse().Return(response)
	// 9 seconds have passed, as the previous time was 1225 and the current is 1234
	resps := make([]*redisSrc.Resp, 2)
	resps[0] = redisSrc.NewResp(int64(1225))
	resps[1] = redisSrc.NewResp(float64(10))
	// Mock the number of tokens in user 1's bucket as 10
	response.EXPECT().Array().Return(resps)

	connection.EXPECT().PipeAppend("HMSET", "domain_key3_value3_", "prevTime", int64(1234), "numTokens", float64(0))
	connection.EXPECT().PipeResponse()

	pool.EXPECT().Put(connection)
	cache := redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0)

	request := common.NewRateLimitRequest("domain", [][][2]string{{{"key3", "value3"}}}, 10)

	limits := []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key3_value3", statsStore, 10)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, TokensRemaining: 0.0}},
		cache.DoLimit(nil, request, limits))

	assert.Equal(uint64(10), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())
	// take 10 off at 18 second, rejected
	pool.EXPECT().Get().Return(connection)

	timeSource.EXPECT().UnixNow().Return(int64(1243))

	connection.EXPECT().PipeAppend("HMGET", "domain_key3_value3_", "prevTime", "numTokens")

	connection.EXPECT().PipeResponse().Return(response)
	// 9 seconds have passed, as the previous time was 1234 and the current is 1243
	resps1 := make([]*redisSrc.Resp, 2)
	resps1[0] = redisSrc.NewResp(int64(1234))
	resps1[1] = redisSrc.NewResp(float64(0))
	// Mock the number of tokens in user 1's bucket as 0
	response.EXPECT().Array().Return(resps1)

	connection.EXPECT().PipeAppend("HMSET", "domain_key3_value3_", "prevTime", int64(1243), "numTokens", float64(9))
	connection.EXPECT().PipeResponse()

	pool.EXPECT().Put(connection)
	cache = redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key3", "value3"}}}, 10)

	limits = []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key3_value3", statsStore, 10)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, TokensRemaining: 9.0}},
		cache.DoLimit(nil, request, limits))
	// bad behavior, right now the weights are treated as seperate requests
	assert.Equal(uint64(20), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(3), limits[0].Stats.NearLimit.Value())

	// take 10 off at 27 second, accepted
	pool.EXPECT().Get().Return(connection)

	timeSource.EXPECT().UnixNow().Return(int64(1252))

	connection.EXPECT().PipeAppend("HMGET", "domain_key3_value3_", "prevTime", "numTokens")

	connection.EXPECT().PipeResponse().Return(response)
	// 9 seconds have passed, as the previous time was 1243 and the current is 1252
	resps2 := make([]*redisSrc.Resp, 2)
	resps2[0] = redisSrc.NewResp(int64(1243))
	resps2[1] = redisSrc.NewResp(float64(9))
	// Mock the number of tokens in user 1's bucket as 9
	response.EXPECT().Array().Return(resps2)

	connection.EXPECT().PipeAppend("HMSET", "domain_key3_value3_", "prevTime", int64(1252), "numTokens", float64(0))
	connection.EXPECT().PipeResponse()

	pool.EXPECT().Put(connection)
	cache = redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key3", "value3"}}}, 10)

	limits = []*config.RateLimit{config.NewRateLimit(1, pb.RateLimitResponse_RateLimit_SECOND, 1, "tokenBucket", "key3_value3", statsStore, 10)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, TokensRemaining: 0.0}},
		cache.DoLimit(nil, request, limits))
	// bad behavior, right now the weights are treated as seperate requests
	assert.Equal(uint64(30), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(4), limits[0].Stats.NearLimit.Value())

}
