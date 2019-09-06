package org.springframework.data.redis.cache;

import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.data.redis.connection.DecoratedRedisConnection;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-05
 */
public class RedisHashCache extends AbstractValueAdaptingCache {

    private String cacheName;
    private String prefix;
    private final RedisOperations redisOperations;

    public RedisHashCache(String name, String prefix, RedisOperations<? extends Object, ? extends Object> redisOperations,
                          long expiration) {
        this(name, prefix, redisOperations, expiration, false);
    }

    /**
     * Create an {@code AbstractValueAdaptingCache} with the given setting.
     *
     * @param allowNullValues whether to allow for {@code null} values
     */
    public RedisHashCache(String name, String prefix, RedisOperations<? extends Object, ? extends Object> redisOperations,
                          long expiration, boolean allowNullValues) {
        super(allowNullValues);
        Assert.hasText(name, "CacheName must not be null or empty!");
        this.cacheName = name;
        this.prefix = prefix;
        this.redisOperations = redisOperations;

        if (allowNullValues) {

            if (redisOperations.getValueSerializer() instanceof StringRedisSerializer
                    || redisOperations.getValueSerializer() instanceof GenericToStringSerializer
                    || redisOperations.getValueSerializer() instanceof JacksonJsonRedisSerializer
                    || redisOperations.getValueSerializer() instanceof Jackson2JsonRedisSerializer) {
                throw new IllegalArgumentException(String.format(
                        "Redis does not allow keys with null value ¯\\_(ツ)_/¯. "
                                + "The chosen %s does not support generic type handling and therefore cannot be used with allowNullValues enabled. "
                                + "Please use a different RedisSerializer or disable null value support.",
                        ClassUtils.getShortName(redisOperations.getValueSerializer().getClass())));
            }
        }
    }

    @Override
    protected Object lookup(Object key) {
        Object[] keys = keys(key);
        if (keys.length == 1) {
            return redisOperations.opsForHash().values(key);
        }
        return redisOperations.opsForHash().get(keys[0], keys[1]);
    }

    @Override
    public String getName() {
        return cacheName;
    }

    @Override
    public Object getNativeCache() {
        return redisOperations;
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        ValueWrapper val = get(key);
        if (val != null) {
            return (T) val.get();
        }

        try {
            return (T) toStoreValue(valueLoader.call());
        } catch (Exception e) {
            e.printStackTrace();
        }


        return null;
    }

    @Override
    public void put(Object key, Object value) {
        if (value instanceof Map) {
            redisOperations.opsForHash().putAll(key, (Map) value);
        } else {
            Object[] keys = keys(key);
            if (value instanceof Collection || value.getClass().isArray()) {
                Map<Object, Object> hashMap = null;
                if (keys.length == 2) {
                    collectionArray2Map(value, (String) keys[1]);
                } else {
                    throw new IllegalArgumentException("redis hash key '" + key + "' invalid, example set= cut:classify:material,id");
                }

                if (hashMap != null) {
                    redisOperations.opsForHash().putAll(keys[0], hashMap);
                }
            } else {
                redisOperations.opsForHash().put(keys[0], keys[1], value);
            }

        }

    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object[] keys = keys(key);
        Boolean bool = redisOperations.opsForHash().putIfAbsent(keys[0], keys[1], value);

        return bool ? null : toWrapper(redisOperations.opsForHash().get(keys[0], keys[1]));
    }

    private ValueWrapper toWrapper(Object value) {
        return (value != null ? new SimpleValueWrapper(value) : null);
    }

    @Override
    public void evict(Object key) {
        Object[] keys = keys(key);
        redisOperations.opsForHash().delete(keys[0], keys[1]);
    }

    @Override
    public void clear() {

    }

    private <T> Map<Object, Object> collectionArray2Map(Object value, String keyFieldName) {
        Map<Object, Object> hashMap = null;
        if (value instanceof Collection) {
            Collection values = (Collection) value;
            hashMap = collection2Map(values, keyFieldName);

        } else if (value.getClass().isArray()) {
            Object[] values = (Object[]) value;
            hashMap = array2Map(values, keyFieldName);
        }
        return hashMap;
    }

    private <T> Map<Object, Object> collection2Map(Collection values, String keyFieldName) {
        Map<Object, Object> hashMap = new HashMap<>();
        for (Object target : values) {
            Object hashKey = getFieldValue(target, keyFieldName);
            hashMap.put(hashKey, target);
        }
        return hashMap;
    }

    private <T> Map<Object, Object> array2Map(Object[] values, String keyFieldName) {
        Map<Object, Object> hashMap = new HashMap<>();
        for (Object target : values) {
            Object hashKey = getFieldValue(target, keyFieldName);
            hashMap.put(hashKey, target);
        }
        return hashMap;
    }

    private <T> Object getFieldValue(Object target, String fieldName) {
        Field field = ReflectionUtils.findField(target.getClass(), fieldName);
        ReflectionUtils.makeAccessible(field);
        Object hashKey = ReflectionUtils.getField(field, target);
        return hashKey;
    }

    private static boolean isClusterConnection(RedisConnection connection) {

        while (connection instanceof DecoratedRedisConnection) {
            connection = ((DecoratedRedisConnection) connection).getDelegate();
        }

        return connection instanceof RedisClusterConnection;
    }

    /**
     * 将长key 变成两级key
     *
     * @param key
     * @return
     */
    private Object[] keys(Object key) {
        if (key instanceof String) {
            return StringUtils.commaDelimitedListToStringArray((String) key);
        }

        return new Object[]{key};
    }

}
