package redis_test

import (
	"math/rand"
	"strconv"
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

func TestRedisSliding(t *testing.T) {
	t.Run("WithoutPerSecondRedis", testRedisSliding(false))
	t.Run("WithPerSecondRedis", testRedisSliding(true))
	t.Run("WithoutPerSecondRedisBurst", testRedisSlidingBurst(false))
	t.Run("WithPerSecondRedisBurst", testRedisSlidingBurst(true))
	t.Run("WithoutPerSecondRedisMany", testRedisSlidingMany(false))
	t.Run("WithPerSecondRedisMany", testRedisSlidingMany(true))
	t.Run("WithoutPerSecondRedistDuration", testRedisSlidingDuration(false))
	t.Run("WithPerSecondRedisDuration", testRedisSlidingDuration(true))
}

// Tests if sliding window algorithm works for all time units
func testRedisSliding(usePerSecondRedis bool) func(*testing.T) {
	return func(t *testing.T) {
		// Mock all objects not directly related to the algorithm logic
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
		}
		pool.EXPECT().Get().Return(connection)

		var connUsed *mock_redis.MockConnection
		if usePerSecondRedis {
			connUsed = perSecondConnection
		} else {
			connUsed = connection
		}

		/* FIRST TEST - Test for 10 request/second time unit
		   This test tests for a request made while the current second window
		   has not been hit with any requests, so there are still 10 requests
		   available for the 2 one second subwindows that can be sent. Since
		   the rate limit has yet to be hit by a request, this one will
		   be successful.
		*/

		// Mock the unix time to be 1234 and send 1 request
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		connUsed.EXPECT().PipeAppend("INCRBY", "domain_key_value_1234", uint32(1))
		// Expire the key after 2 seconds (1 second + 1 second)
		connUsed.EXPECT().PipeAppend("EXPIRE", "domain_key_value_1234", int64(2))

		connUsed.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connUsed.EXPECT().PipeResponse()

		// Use MGET to retrieve values from two previous subwindows
		connUsed.EXPECT().PipeExec("MGET", []string{"domain_key_value_1234", "domain_key_value_1233"}).Return(response)
		// Only the first response should have 1 response (the one we just made)
		response.EXPECT().Array().Return([]*redisSrc.Resp{redisSrc.NewResp(1), redisSrc.NewResp(0)})

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request := common.NewRateLimitRequest("domain", [][][2]string{{{"key", "value"}}}, 1)
		// Create a rate limit of 10 requests/second
		limits := []*config.RateLimit{config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_SECOND, 1, "slidingWindow", "key_value", statsStore)}

		// Request is allowed
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 9}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

		/* SECOND TEST - Test for 10 request/minute time unit
		   This test tests for a request made while the current minute window
		   has been hit with 10 requests, so there are 0 requests
		   available for the 61 one second subwindows that can be sent. Since
		   the rate limit is at capacity, this one should be denied.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the unix time to be 1234 and send 1 request
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		connection.EXPECT().PipeAppend("INCRBY", "domain_key2_value2_subkey2_subvalue2_1234", uint32(1))
		// Expire the key after 61 seconds (1 minute + 1 second)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key2_value2_subkey2_subvalue2_1234", int64(61))
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys := make([]string, 61)
		// resps is the return value of MGET, an array representing past requests
		resps := make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key2_value2_subkey2_subvalue2_" + strconv.Itoa(1234-i)
			// The past 10 seconds have requests, plus the 11th one just now
			if i <= 10 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)
		connection.EXPECT().PipeExec("DECRBY", "domain_key2_value2_subkey2_subvalue2_1234", uint32(1))
		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key2", "value2"}},
				{{"key2", "value2"}, {"subkey2", "subvalue2"}},
			}, 1)
		// The first request has no rate limit, the second is 10 requests/minute
		limits = []*config.RateLimit{
			nil,
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_MINUTE, 1, "slidingWindow", "key2_value2_subkey2_subvalue2", statsStore)}
		// First request is allowed (because no limit), the second is over limit
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: nil, LimitRemaining: 0},
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[1].Limit, LimitRemaining: 0}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(2), limits[1].Stats.NearLimit.Value())

		/* THIRD TEST - Test for 10 request/hour and 10 request/day time unit
		   The hour test tests for a request made while the current hour window
		   has been hit with 10 requests, so there are 0 requests
		   available for the 61 one minute subwindows that can be sent. Since
		   the rate limit is at capacity, this one should be denied.
		   The day test tests for a request made while the current day window
		   has been hit with 12 requests, so there are 0 requests available for
		   the for the 13 one hour subwindows that can be sent.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)
		// Mock the unix time to be 1000000
		timeSource.EXPECT().UnixNow().Return(int64(1000000))
		// Send 1 request for current hour window (999960 represents start of hour)
		connection.EXPECT().PipeAppend("INCRBY", "domain_key3_value3_999960", uint32(1))
		// Expect to expire the first key after 3660 seconds (1 hour + 1 minute)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key3_value3_999960", int64(3660))
		// Send 1 request for current day window (997200 represents start of day)
		connection.EXPECT().PipeAppend("INCRBY", "domain_key3_value3_subkey3_subvalue3_997200", uint32(1))
		// Expect to expire the second key after 90000 seconds (1 day + 1 hour)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key3_value3_subkey3_subvalue3_997200", int64(90000))

		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys = make([]string, 61)
		// resps is the return value of MGET, an array representing past requests
		resps = make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key3_value3_" + strconv.Itoa(999960-(i*60))
			// The past 10 minutes have requests, plus the 11th one just now
			if i <= 10 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		keys = make([]string, 25)
		resps = make([]*redisSrc.Resp, 25)
		for i := 0; i <= 24; i++ {
			keys[i] = "domain_key3_value3_subkey3_subvalue3_" + strconv.Itoa(997200-(i*3600))
			// The past 10 hours have requests, plus the 11th one just now
			if i <= 10 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("DECRBY", "domain_key3_value3_999960", uint32(1))
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)
		connection.EXPECT().PipeExec("DECRBY", "domain_key3_value3_subkey3_subvalue3_997200", uint32(1))

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key3", "value3"}},
				{{"key3", "value3"}, {"subkey3", "subvalue3"}},
			}, 1)
		// Rate limits are 10 requests/hour and 10 requests/day
		limits = []*config.RateLimit{
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_HOUR, 1, "slidingWindow", "key3_value3", statsStore),
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_DAY, 1, "slidingWindow", "key3_value3_subkey3_subvalue3", statsStore)}
		// Both requests are rejected because over limit
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0},
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[1].Limit, LimitRemaining: 0}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(2), limits[0].Stats.NearLimit.Value())
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(2), limits[0].Stats.NearLimit.Value())
	}
}

/* Tests if sliding window algorithm works when bursts of requests are sent.
   These tests should deny requests where a fixed window algorithm would allow.
*/
func testRedisSlidingBurst(usePerSecondRedis bool) func(*testing.T) {
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

		/* FOURTH TEST - Burst test for 10 request/minute time unit
		   This test tests for a request made while the current minute window
		   has been hit with 9 requests, so there is 1 request
		   available for the 61 one second subwindows that can be sent. Since
		   the rate limit is not at capacity, this one should be allowed.
		   56 seconds pass, and although it is not the same minute anymore, the
		   subwindows allow the past 61 seconds from current time to be
		   captured. This means that after 56 seconds, there are 5 old and 9
		   recent requests that are sent. This results in a new request being
		   denied, where it would've passed in fixed window.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)
		// Mock the unix time to be 1234 and send 1 request
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		connection.EXPECT().PipeAppend("INCRBY", "domain_key_value_1234", uint32(1))
		// Expect to expire after 61 seconds (1 minute + 1 second)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key_value_1234", int64(61))

		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys := make([]string, 61)
		// resps is the return value of MGET, an array representing past requests
		resps := make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key_value_" + strconv.Itoa(1234-i)
			// Most recent 9 subwindows each have 1 request
			if i <= 8 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request := common.NewRateLimitRequest("domain", [][][2]string{{{"key", "value"}}}, 1)
		// Create a rate limit of 10 requests/minute
		limits := []*config.RateLimit{config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_MINUTE, 1, "slidingWindow", "key_value", statsStore)}

		// Request is approved
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 1}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

		// 56 seconds pass

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// New mock unix time is 1290 and send 1 request
		timeSource.EXPECT().UnixNow().Return(int64(1290))
		connection.EXPECT().PipeAppend("INCRBY", "domain_key_value_1290", uint32(1))
		connection.EXPECT().PipeAppend("EXPIRE", "domain_key_value_1290", int64(61))

		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys = make([]string, 61)
		resps = make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key_value_" + strconv.Itoa(1290-i)
			// 1 recent subwindow and 5 subwindows 1230-1234 have responses
			if i <= 0 || i >= 56 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)
		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

                // Request is allowed because 5 requests made in the past and 1 current ( 5 + 1 <= 10)
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 4}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(2), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())
	}
}

// Tests if algorithm can process requests for different rate limits at once
func testRedisSlidingMany(usePerSecondRedis bool) func(*testing.T) {
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

		/* FIFTH TEST - This test tests if the rate limiter has the ability to
		   process different types of rate limits at a time. Here, a request is
		   sent that has both an hour and day limit.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the unix time to be 1000000
		timeSource.EXPECT().UnixNow().Return(int64(1000000))
		// Send 1 request for current hour window (999960 represents start of hour)
		connection.EXPECT().PipeAppend("INCRBY", "domain_key1_value1_999960", uint32(1))
		// Expect to expire after 3660 seconds (1 hour + 1 minute)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key1_value1_999960", int64(3660))
		// Send 1 request for current day window (999960 represents start of day)
		connection.EXPECT().PipeAppend("INCRBY", "domain_key2_value2_997200", uint32(1))
		// Expire after 90000 seconds (1 day + 1 hour)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key2_value2_997200", int64(90000))
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys := make([]string, 61)
		// resps is the return value of MGET, an array representing past requests
		resps := make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key1_value1_" + strconv.Itoa(999960-(i*60))
			// Only the most recent minute has a request (the one just now)
			if i == 0 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		// Repeat for other rate limit
		keys = make([]string, 25)
		resps = make([]*redisSrc.Resp, 25)
		for i := 0; i <= 24; i++ {
			keys[i] = "domain_key2_value2_" + strconv.Itoa(997200-(i*3600))
			if i == 0 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request := common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key1", "value1"}},
				{{"key2", "value2"}},
			}, 1)
		// Create rate limits of 10 requests/hour and 10 requests/day
		limits := []*config.RateLimit{
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_HOUR, 1, "slidingWindow", "key1_value1", statsStore),
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_DAY, 1, "slidingWindow", "key2_value2", statsStore)}
		// Both requests are accepted
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 9},
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[1].Limit, LimitRemaining: 9}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
		assert.Equal(uint64(1), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[1].Stats.NearLimit.Value())

		/* SIXTH TEST - This test tests if the rate limiter can process multiple
		requests to the same rate limit at once. Here, a two requests are
		processed at the same time.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		// Mock the unix time to be 1000000
		timeSource.EXPECT().UnixNow().Return(int64(1000000))
		// Send 1 request for current hour window (999960 represents start of hour)
		connection.EXPECT().PipeAppend("INCRBY", "domain_key3_value3_999960", uint32(1))
		// Expect to expire after 3660 seconds (1 hour + 1 minute)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key3_value3_999960", int64(3660))
		// Send 1 request for current hour window (999960 represents start of hour)
		connection.EXPECT().PipeAppend("INCRBY", "domain_key3_value3_999960", uint32(1))
		// Expire after 3660 seconds (1 hour + 1 minute)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key3_value3_999960", int64(3660))
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys = make([]string, 61)
		// resps is the return value of MGET, an array representing past requests
		resps = make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key3_value3_" + strconv.Itoa(999960-(i*60))
			// Only the most recent minute has requests (the three just now)
			if i == 0 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		// Repeat for second request
		keys = make([]string, 61)
		resps = make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key3_value3_" + strconv.Itoa(999960-(i*60))
			// Only the most recent minute has requests
			if i == 0 {
				resps[i] = redisSrc.NewResp(2)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key3", "value3"}},
				{{"key3", "value3"}},
			}, 1)
		// Create rate limit of 10 requests/hour, repeat twice because we are calling it twice
		limits = []*config.RateLimit{
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_HOUR, 1, "slidingWindow", "key3_value3", statsStore),
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_HOUR, 1, "slidingWindow", "key3_value3", statsStore)}
		// Both requests are accepted
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 9},
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[1].Limit, LimitRemaining: 8}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(2), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
		assert.Equal(uint64(2), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[1].Stats.NearLimit.Value())
	}
}

// Test if algorithm works with limits of different durations
func testRedisSlidingDuration(usePerSecondRedis bool) func(*testing.T) {
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

		/* SEVENTH TEST - This test tests if the rate limiter has the ability to
		   process multiple durations for requests at a time. Here, a request is
		   sent that has both an 2 requests/hour and 4 requests/day limit.
		*/

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		}
		pool.EXPECT().Get().Return(connection)

		timeSource.EXPECT().UnixNow().Return(int64(1000000))
		// Send 1 request for current two-minute subwindow
		connection.EXPECT().PipeAppend("INCRBY", "domain_key1_value1_999960", uint32(1))
		// Expect to expire after 7320 seconds (2 hours + 2 minutes)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key1_value1_999960", int64(7320))
		// Send 1 request for current four-hour window
		connection.EXPECT().PipeAppend("INCRBY", "domain_key2_value2_993600", uint32(1))
		// Expect to expire after 360000 seconds (4 days + 4 hours)
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key2_value2_993600", int64(360000))
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(1))
		connection.EXPECT().PipeResponse()

		keys := make([]string, 61)
		// resps is the return value of MGET, an array representing past requests
		resps := make([]*redisSrc.Resp, 61)
		for i := 0; i <= 60; i++ {
			keys[i] = "domain_key1_value1_" + strconv.Itoa(999960-(i*120))
			// Only most recent subwindow (length of 2 minutes) has a request
			if i == 0 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		// Repeat for other rate limit
		keys = make([]string, 25)
		resps = make([]*redisSrc.Resp, 25)
		for i := 0; i <= 24; i++ {
			keys[i] = "domain_key2_value2_" + strconv.Itoa(993600-(i*14400))
			// Only most recent subwindow (length of 4 hours) has a request
			if i == 0 {
				resps[i] = redisSrc.NewResp(1)
			} else {
				resps[i] = redisSrc.NewResp(0)
			}
		}
		connection.EXPECT().PipeExec("MGET", keys).Return(response)
		response.EXPECT().Array().Return(resps)

		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		}
		pool.EXPECT().Put(connection)

		request := common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key1", "value1"}},
				{{"key2", "value2"}},
			}, 1)
		// Rate limits are 10 requests per two hours and 10 requests per four days
		limits := []*config.RateLimit{
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_HOUR, 2, "slidingWindow", "key1_value1", statsStore),
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_DAY, 4, "slidingWindow", "key2_value2", statsStore)}
		// Both requests are accepted
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 9},
				{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[1].Limit, LimitRemaining: 9}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
		assert.Equal(uint64(1), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[1].Stats.NearLimit.Value())
	}
}
