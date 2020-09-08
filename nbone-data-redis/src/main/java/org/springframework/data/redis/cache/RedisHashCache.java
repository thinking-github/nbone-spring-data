package org.springframework.data.redis.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.RedisHashKey;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DecoratedRedisConnection;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-05
 */
public class RedisHashCache extends RedisCacheX {

    private static final Logger logger = LoggerFactory.getLogger(RedisHashCache.class);
    public static final String FIELD_REFERENCE = "@";

    private final RedisOperations redisOperations;
    private Class<?> hashKeyType;


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

        super(name, prefix, redisOperations, expiration, allowNullValues);
        this.redisOperations = redisOperations;

    }

    public RedisHashCache hashKeyType(Class<?> hashKeyType) {
        this.hashKeyType = hashKeyType;
        return this;
    }

    @Override
    public String getName() {
        return super.getName();
    }

    @Override
    public Object getNativeCache() {
        return redisOperations;
    }

    @Override
    protected Object lookup(Object key) {
        RedisHashKey redisHashKey = redisHashKey(key);
        if (redisHashKey.getHashKeys() != null) {
            return hashMultiGet(redisHashKey.getKey(), redisHashKey.getHashKeys());
        } else if (redisHashKey.getHashKey() == null || containsFieldReference(redisHashKey.getHashKey())) {
            if (redisHashKey.isHasContext()) {
                if (redisHashKey.isReturnTypeMap()) {
                    return hashEntries(redisHashKey.getKey());
                } else if (redisHashKey.isReturnTypeList()) {
                    return hashValues(redisHashKey.getKey());
                } else if (redisHashKey.isReturnTypeArray()) {
                    return hashValues(redisHashKey.getKey()).toArray();
                } else {
                    return hashValues(redisHashKey.getKey());
                }
            } else {
                return hashValues(redisHashKey.getKey());
            }
        }
        return hashGet(redisHashKey.getKey(), redisHashKey.getHashKey());
    }

    @Override
    public <T> T get(Object key, Class<T> type) {

        ValueWrapper wrapper = get(key);
        return wrapper == null ? null : (T) wrapper.get();
    }

    @Override
    public ValueWrapper get(Object key) {
        Object value = lookup(key);
        if (ObjectUtils.isEmpty(value)) {
            return null;
        }
        return new SimpleValueWrapper(fromStoreValue(value));
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
        if (value == null) {
            return;
        }
        RedisHashKey redisHashKey = redisHashKey(key);
        if (value instanceof Map) {
            hashPutAll(redisHashKey.getKey(), (Map<Object, Object>) value);
        } else {
            if (value instanceof Collection || value.getClass().isArray()) {
                Map<Object, Object> hashMap = null;
                if (redisHashKey.getHashKey() != null) {
                    String hashKeyField = processFieldReference(redisHashKey.getHashKey());
                    hashMap = collectionArray2Map(value, hashKeyField);
                } else {
                    throw new IllegalArgumentException("redis hash key '" + key + "' invalid, example set= cut:classify:material,id");
                }
                hashPutAll(redisHashKey.getKey(), hashMap);

            } else {
                hashPut(redisHashKey.getKey(), redisHashKey.getHashKey(), value);
            }

        }

    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        RedisHashKey redisHashKey = redisHashKey(key);
        Boolean bool = hashPutIfAbsent(redisHashKey.getKey(), redisHashKey.getHashKey(), value);
        return bool ? null : toWrapper(hashGet(redisHashKey.getKey(), redisHashKey.getHashKey()));
    }

    private ValueWrapper toWrapper(Object value) {
        return (value != null ? new SimpleValueWrapper(value) : null);
    }

    @Override
    public void evict(Object key) {
        RedisHashKey redisHashKey = redisHashKey(key);

        if (redisHashKey.getHashKeys() != null) {
            redisOperations.opsForHash().delete(redisHashKey.getKey(), redisHashKey.getHashKeys().toArray());
        } else if (redisHashKey.getHashKey() == null || containsFieldReference(redisHashKey.getHashKey())) {

            delete(redisHashKey.getKey());
            return;
        }

        redisOperations.opsForHash().delete(redisHashKey.getKey(), redisHashKey.getHashKeys());
    }

    @Override
    public void clear() {
        super.clear();
        //logger.error("unsupported clear operation. thinking");
    }


    private void hashPutAll(Object key, Map<Object, Object> hashMap) {
        if (ObjectUtils.isEmpty(hashMap)) {
            return;
        }
        redisOperations.opsForHash().putAll(key, hashMap);
        processKeyExpiration(key);
        maintainKnownKeys(key);

    }

    private void hashPut(Object key, Object hashKey, Object value) {
        redisOperations.opsForHash().put(key, hashKey, value);
        processKeyExpiration(key);
        maintainKnownKeys(key);

    }

    private Boolean hashPutIfAbsent(Object key, Object hashKey, Object value) {
        Boolean bool = redisOperations.opsForHash().putIfAbsent(key, hashKey, value);
        if (bool) {
            processKeyExpiration(key);
            maintainKnownKeys(key);
        }
        return bool;
    }

    private <T> List<T> hashValues(Object key) {
        return redisOperations.opsForHash().values(key);
    }

    private <K, V> Map<K, V> hashEntries(Object key) {
        return redisOperations.opsForHash().entries(key);
    }

    private <T> List<T> hashMultiGet(Object key, Collection<?> fields) {
        return redisOperations.opsForHash().multiGet(key, fields);
    }

    private Object hashGet(Object key, Object hashKey) {
        return redisOperations.opsForHash().get(key, hashKey);
    }

    private void delete(Object key) {
        redisOperations.delete(key);
        cleanKnownKeys(key);
    }


    protected void processKeyExpiration(Object key) {
        if (getCacheMetadata().getDefaultExpiration() > 0) {
            redisOperations.expire(key, getCacheMetadata().getDefaultExpiration(), TimeUnit.SECONDS);
        }
    }

    protected void maintainKnownKeys(Object key) {
        if (!hasKeyPrefix()) {
            byte[] keyBytes = redisOperations.getKeySerializer().serialize(key);
            redisOperations.execute(new RedisCallback() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    connection.zAdd(getCacheMetadata().getSetOfKnownKeysKey(), 0, keyBytes);

                    if (!isEternal()) {
                        connection.expire(getCacheMetadata().getSetOfKnownKeysKey(), getTimeToLive());
                    }
                    return null;
                }
            });


        }
    }

    protected void cleanKnownKeys(Object key) {
        if (!hasKeyPrefix()) {
            byte[] keyBytes = redisOperations.getKeySerializer().serialize(key);
            redisOperations.execute(new RedisCallback() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    connection.zRem(getCacheMetadata().getSetOfKnownKeysKey(), keyBytes);
                    return null;
                }
            });
        }
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
        if (ObjectUtils.isEmpty(values)) {
            return null;
        }
        Map<Object, Object> hashMap = new HashMap<>();
        for (Object target : values) {
            Object hashKey = getFieldValue(target, keyFieldName);
            hashMap.put(hashKey, target);
        }
        return hashMap;
    }

    private <T> Map<Object, Object> array2Map(Object[] values, String keyFieldName) {
        if (ObjectUtils.isEmpty(values)) {
            return null;
        }
        Map<Object, Object> hashMap = new HashMap<>();
        for (Object target : values) {
            Object hashKey = getFieldValue(target, keyFieldName);
            hashMap.put(hashKey, target);
        }
        return hashMap;
    }

    private <T> Object getFieldValue(Object target, String fieldName) {
        Field field = ReflectionUtils.findField(target.getClass(), fieldName);
        if (field == null) {
            throw new IllegalArgumentException("class [" + target.getClass().getName() + "] Cannot resolve field: " + fieldName);
        }
        ReflectionUtils.makeAccessible(field);
        Object hashKey = ReflectionUtils.getField(field, target);
        return hashKey;
    }

    private <T> boolean containsField(Class<T> targetClass, String fieldName) {
        Field field = ReflectionUtils.findField(targetClass, fieldName);
        return field == null ? false : true;
    }

    private <T> boolean containsFieldReference(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof String && ((String) value).startsWith(FIELD_REFERENCE)) {
            return true;
        }
        return false;
    }

    private <T> String processFieldReference(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String && ((String) value).startsWith(FIELD_REFERENCE)) {
            return ((String) value).substring(1);
        }
        return value.toString();
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
    private RedisHashKey redisHashKey(Object key) {
        if (key instanceof RedisHashKey) {
            RedisHashKey redisHashKey = (RedisHashKey) key;
            if (usesKeyPrefix()) {
                redisHashKey.setKey(prefixNamespace() + redisHashKey.getKey());
            }
            return redisHashKey;
        }

        Object keyUsed = key;
        if (usesKeyPrefix()) {
            keyUsed = prefixNamespace() + key;
        }
        //thinking:classify:packageName,12
        //thinking:classify:packageName,12,13,15
        if (keyUsed instanceof String) {
            //String[] keys = StringUtils.commaDelimitedListToStringArray((String) keyUsed);
            return new RedisHashKey((String) keyUsed);
        }

        return new RedisHashKey(key, null);
    }

}
