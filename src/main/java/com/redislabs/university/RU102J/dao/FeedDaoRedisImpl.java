package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.MeterReading;
import redis.clients.jedis.*;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeedDaoRedisImpl implements FeedDao {

    public static final int THREE_DAYS_SECONDS = 3 * 24 * 60 * 60;
    public static final int TWO_WEEKS_SECONDS = 14 * 24 * 60 * 60;
    private final JedisPool jedisPool;
    private static final long globalMaxFeedLength = 10000;
    private static final long siteMaxFeedLength = 2440;

    public FeedDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // Challenge #6
    @Override
    public void insert(MeterReading meterReading) {
        // START Challenge #6
        // insert to global where id is siteId
        try(Jedis jedis = jedisPool.getResource()) {
            Long siteId = meterReading.getSiteId();

            String globalFeedKey = RedisSchema.getGlobalFeedKey();
            Map<String, String> meterReadingMap = meterReading.toMap();
            jedis.xadd(globalFeedKey, StreamEntryID.NEW_ENTRY, meterReadingMap);
            jedis.expire(globalFeedKey, THREE_DAYS_SECONDS);

            String feedKey = RedisSchema.getFeedKey(siteId);
            jedis.xadd(feedKey, StreamEntryID.NEW_ENTRY, meterReadingMap);
            jedis.expire(globalFeedKey, TWO_WEEKS_SECONDS);
        }
        // inset to local feed where id is siteId
        // END Challenge #6
    }

    @Override
    public List<MeterReading> getRecentGlobal(int limit) {
        return getRecent(RedisSchema.getGlobalFeedKey(), limit);
    }

    @Override
    public List<MeterReading> getRecentForSite(long siteId, int limit) {
        return getRecent(RedisSchema.getFeedKey(siteId), limit);
    }

    public List<MeterReading> getRecent(String key, int limit) {
        List<MeterReading> readings = new ArrayList<>(limit);
        try (Jedis jedis = jedisPool.getResource()) {
            List<StreamEntry> entries = jedis.xrevrange(key, null,
                    null, limit);
            for (StreamEntry entry : entries) {
                readings.add(new MeterReading(entry.getFields()));
            }
            return readings;
        }
    }
}
