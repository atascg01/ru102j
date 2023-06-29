package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.*;

import java.sql.Date;
import java.time.Instant;
import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String key = RedisSchema.getSlidingWindowRateLimiterKey(windowSizeMS, name, maxHits);
        try(Jedis jedis = jedisPool.getResource()){
            Transaction t = jedis.multi();
            long nowMillis = ZonedDateTime.now().toInstant().toEpochMilli();
            String member = String.valueOf(nowMillis) + Math.random();
            t.zadd(key, nowMillis, member);
            t.zremrangeByScore(key, 0, nowMillis - windowSizeMS);
            Response<Long> totalRequests = t.zcard(key);

            t.exec();
            if (totalRequests.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
