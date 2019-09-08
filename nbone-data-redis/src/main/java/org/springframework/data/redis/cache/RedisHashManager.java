package org.springframework.data.redis.cache;

import org.springframework.data.redis.core.RedisOperations;

import java.util.Collection;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019-09-07
 */
public class RedisHashManager extends RedisCacheManager {

    private final boolean cacheNullValues;

    public RedisHashManager(RedisOperations redisOperations) {
        super(redisOperations);
        this.cacheNullValues = false;
    }

    public RedisHashManager(RedisOperations redisOperations, Collection<String> cacheNames) {
        super(redisOperations, cacheNames);
        this.cacheNullValues = false;
    }

    public RedisHashManager(RedisOperations redisOperations, Collection<String> cacheNames, boolean cacheNullValues) {
        super(redisOperations, cacheNames, cacheNullValues);
        this.cacheNullValues = cacheNullValues;
    }


    @Override
    @SuppressWarnings("unchecked")
    protected RedisCache createCache(String cacheName) {
        long expiration = computeExpiration(cacheName);
        String prefix = (isUsePrefix() ? cacheName : null);
        return new RedisHashCache(cacheName, prefix, getRedisOperations(), expiration, cacheNullValues);
    }
}
