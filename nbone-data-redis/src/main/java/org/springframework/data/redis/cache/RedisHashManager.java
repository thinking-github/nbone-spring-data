package org.springframework.data.redis.cache;

import org.springframework.data.redis.core.RedisOperations;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019-09-07
 */
public class RedisHashManager extends RedisCacheManager {

    private final boolean cacheNullValues;

    /**
     * hashKey type 当method返回map 时使用, 作为map的key type
     */
    private Map<String, Class<?>> hashKeyTypes = null;

    public RedisHashManager(RedisOperations redisOperations) {
        this(redisOperations, Collections.emptyList());
    }

    public RedisHashManager(RedisOperations redisOperations, Collection<String> cacheNames) {
        this(redisOperations, cacheNames,false);
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
        Class<?> hashKeyType = getHashKeyType(cacheName);
        return new RedisHashCache(cacheName, prefix, getRedisOperations(), expiration, cacheNullValues).hashKeyType(hashKeyType);
    }

    public void setHashKeyTypes(Map<String, Class<?>> hashKeyTypes) {
        this.hashKeyTypes = hashKeyTypes;
    }

    public <T> RedisHashManager addHashKeyType(String cacheName, Class<T> type) {
        if (hashKeyTypes == null) {
            hashKeyTypes = new HashMap<>();
        }
        hashKeyTypes.put(cacheName, type);
        return this;
    }

    public <T> Class<T> getHashKeyType(String cacheName) {
        if (hashKeyTypes != null) {
            return (Class<T>) hashKeyTypes.get(cacheName);
        }
        return null;
    }

}
