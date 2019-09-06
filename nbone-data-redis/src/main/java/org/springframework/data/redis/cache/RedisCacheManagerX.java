package org.springframework.data.redis.cache;

import org.springframework.data.redis.core.RedisOperations;

import java.util.Collection;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-03
 */
public class RedisCacheManagerX extends RedisCacheManager {

    private final boolean cacheNullValues;

    public RedisCacheManagerX(RedisOperations redisOperations) {
        super(redisOperations);
        this.cacheNullValues = false;
    }

    public RedisCacheManagerX(RedisOperations redisOperations, Collection<String> cacheNames) {
        super(redisOperations, cacheNames);
        this.cacheNullValues = false;
    }

    public RedisCacheManagerX(RedisOperations redisOperations, Collection<String> cacheNames, boolean cacheNullValues) {
        super(redisOperations, cacheNames, cacheNullValues);
        this.cacheNullValues = cacheNullValues;
    }


    @Override
    @SuppressWarnings("unchecked")
    protected RedisCache createCache(String cacheName) {
        long expiration = computeExpiration(cacheName);
        byte[] prefix = (isUsePrefix() ? getCachePrefix().prefix(cacheName) : null);
        return new RedisCacheX(cacheName, prefix, getRedisOperations(), expiration, cacheNullValues);
    }
}
