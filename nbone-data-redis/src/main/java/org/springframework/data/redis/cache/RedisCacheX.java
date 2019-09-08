/*
 * Copyright 2011-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.cache;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;

import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DecoratedRedisConnection;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Cache implementation on top of Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
public class RedisCacheX extends RedisCache {

    private String cacheName;
    private String prefix;

    @SuppressWarnings("rawtypes") //
    private final RedisOperations redisOperations;
    private final RedisCacheMetadata cacheMetadata;
    private final CacheValueAccessor cacheValueAccessor;


    public RedisCacheX(String name, String prefix, RedisOperations<? extends Object, ? extends Object> redisOperations,
                       long expiration) {
        this(name, prefix, redisOperations, expiration, false);
    }


    public RedisCacheX(String name, String prefix, RedisOperations<? extends Object, ? extends Object> redisOperations,
                       long expiration, boolean allowNullValues) {

        this(name, prefix != null ? RedisUtils.DEFAULT_STRING_SERIALIZER.serialize(prefix.concat(":")) : null,
                redisOperations, expiration, allowNullValues);
        this.cacheName = name;
        this.prefix = prefix;
    }

    private RedisCacheX(String name, byte[] prefix, RedisOperations<?, ?> redisOperations, long expiration, boolean allowNullValues) {
        super(name, prefix, redisOperations, expiration, allowNullValues);

        RedisSerializer<?> serializer = redisOperations.getValueSerializer() != null ? redisOperations.getValueSerializer()
                : (RedisSerializer<?>) new JdkSerializationRedisSerializer();

        this.cacheMetadata = new RedisCacheMetadata(name, prefix);
        this.cacheMetadata.setDefaultExpiration(expiration);
        this.redisOperations = redisOperations;
        this.cacheValueAccessor = new CacheValueAccessor(serializer);
    }

    // new add thinking
    public RedisOperations getRedisOperations() {
        return redisOperations;
    }

    // new add thinking
    protected RedisCacheMetadata getCacheMetadata() {
        return cacheMetadata;
    }

    public String getPrefix() {
        return prefix;
    }

    public boolean usesKeyPrefix() {
        return (prefix != null && prefix.length() > 0);
    }
    public boolean hasKeyPrefix(){
        return usesKeyPrefix();
    }
    public boolean isEternal() {
        return 0 == cacheMetadata.getDefaultExpiration();
    }
    public long getTimeToLive() {
        return cacheMetadata.getDefaultExpiration();
    }

    public String prefixNamespace() {
        return prefix.concat(":");
    }

    /**
     * Return the value to which this cache maps the specified key, generically specifying a type that return value will
     * be cast to.
     *
     * @param key
     * @param type
     * @return
     * @see DATAREDIS-243
     */
    public <T> T get(Object key, Class<T> type) {

        ValueWrapper wrapper = get(key);
        return wrapper == null ? null : (T) wrapper.get();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.Cache#get(java.lang.Object)
     */
    @Override
    public ValueWrapper get(Object key) {
        return get(cacheMetadata.getKeyPrefix(), key);
    }

    /*
     * @see  org.springframework.cache.Cache#get(java.lang.Object, java.util.concurrent.Callable)
     * introduced in springframework 4.3.0.RC1
     */
    public <T> T get(final Object key, final Callable<T> valueLoader) {

        RedisCacheElementX cacheElement = new RedisCacheElementX(cacheMetadata.getKeyPrefix(), key,
                new StoreTranslatingCallable(valueLoader))
                .expireAfter(cacheMetadata.getDefaultExpiration())
                .withKeySerializer(redisOperations.getKeySerializer());
        BinaryRedisCacheElement rce = new BinaryRedisCacheElement(cacheElement, cacheValueAccessor);

        ValueWrapper val = get(key);
        if (val != null) {
            return (T) val.get();
        }

        RedisWriteThroughCallback callback = new RedisWriteThroughCallback(rce, cacheMetadata);

        try {
            byte[] result = (byte[]) redisOperations.execute(callback);
            return (T) (result == null ? null : fromStoreValue(cacheValueAccessor.deserializeIfNecessary(result)));
        } catch (RuntimeException e) {
            throw CacheValueRetrievalExceptionFactory.INSTANCE.create(key, valueLoader, e);
        }
    }

    /**
     * Return the value to which this cache maps the specified key.
     *
     * @param cacheKey the key whose associated value is to be returned via its binary representation.
     * @return the {@link RedisCacheElement} stored at given key or {@literal null} if no value found for key.
     * @since 1.5
     */
    public RedisCacheElementX get(byte[] prefix, final Object cacheKey) {

        Assert.notNull(cacheKey, "CacheKey must not be null!");

        Boolean exists = (Boolean) redisOperations.execute(new RedisCallback<Boolean>() {

            @Override
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.exists(getRedisCacheKey(prefix, cacheKey));
            }
        });

        if (!exists) {
            return null;
        }

        byte[] bytes = doLookup(prefix, cacheKey);

        // safeguard if key gets deleted between EXISTS and GET calls.
        if (bytes == null) {
            return null;
        }

        return new RedisCacheElementX(prefix, cacheKey, fromStoreValue(deserialize(bytes)))
                .withKeySerializer(redisOperations.getKeySerializer());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
     */
    @Override
    public void put(final Object key, final Object value) {

        put(new RedisCacheElementX(cacheMetadata.getKeyPrefix(), key, toStoreValue(value))
                .expireAfter(cacheMetadata.getDefaultExpiration())
                .withKeySerializer(redisOperations.getKeySerializer()));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.support.AbstractValueAdaptingCache#fromStoreValue(java.lang.Object)
     */
    @Override
    protected Object fromStoreValue(Object storeValue) {

        // we need this override for the GenericJackson2JsonRedisSerializer support.
        if (isAllowNullValues() && storeValue instanceof NullValue) {
            return null;
        }

        return super.fromStoreValue(storeValue);
    }

    /**
     * Add the element by adding {@link RedisCacheElement#get()} at {@link RedisCacheElement#getKeyBytes()}. If the cache
     * previously contained a mapping for this {@link RedisCacheElement#getKeyBytes()}, the old value is replaced by
     * {@link RedisCacheElement#get()}.
     *
     * @param element must not be {@literal null}.
     * @since 1.5
     */
    public void put(RedisCacheElementX element) {

        Assert.notNull(element, "Element must not be null!");

        redisOperations
                .execute(new RedisCachePutCallback(new BinaryRedisCacheElement(element, cacheValueAccessor), cacheMetadata));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.Cache#putIfAbsent(java.lang.Object, java.lang.Object)
     */
    public ValueWrapper putIfAbsent(Object key, final Object value) {

        return putIfAbsent(new RedisCacheElementX(cacheMetadata.getKeyPrefix(), key, toStoreValue(value))
                .expireAfter(cacheMetadata.getDefaultExpiration())
                .withKeySerializer(redisOperations.getKeySerializer()));
    }

    /**
     * Add the element as long as no element exists at {@link RedisCacheElement#getKeyBytes()}. If a value is present for
     * {@link RedisCacheElement#getKeyBytes()} this one is returned.
     *
     * @param element must not be {@literal null}.
     * @return
     * @since 1.5
     */
    public ValueWrapper putIfAbsent(RedisCacheElementX element) {

        Assert.notNull(element, "Element must not be null!");

        new RedisCachePutIfAbsentCallback(new BinaryRedisCacheElement(element, cacheValueAccessor), cacheMetadata);

        return toWrapper(cacheValueAccessor.deserializeIfNecessary((byte[]) redisOperations.execute(
                new RedisCachePutIfAbsentCallback(new BinaryRedisCacheElement(element, cacheValueAccessor), cacheMetadata))));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.Cache#evict(java.lang.Object)
     */
    public void evict(Object key) {
        evict(new RedisCacheElementX(cacheMetadata.getKeyPrefix(), key, null)
                .withKeySerializer(redisOperations.getKeySerializer()));
    }

    /**
     * @param element {@link RedisCacheElement#getKeyBytes()}
     * @since 1.5
     */
    public void evict(final RedisCacheElementX element) {

        Assert.notNull(element, "Element must not be null!");
        redisOperations
                .execute(new RedisCacheEvictCallback(new BinaryRedisCacheElement(element, cacheValueAccessor), cacheMetadata));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.Cache#clear()
     */
    public void clear() {
        redisOperations.execute(cacheMetadata.usesKeyPrefix() ? new RedisCacheCleanByPrefixCallback(cacheMetadata)
                : new RedisCacheCleanByKeysCallback(cacheMetadata));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.Cache#getName()
     */
    public String getName() {
        return cacheMetadata.getCacheName();
    }

    /**
     * {@inheritDoc} This implementation simply returns the RedisTemplate used for configuring the cache, giving access to
     * the underlying Redis store.
     */
    public Object getNativeCache() {
        return redisOperations;
    }

    private ValueWrapper toWrapper(Object value) {
        return (value != null ? new SimpleValueWrapper(value) : null);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.cache.support.AbstractValueAdaptingCache#lookup(java.lang.Object)
     */
    @Override
    protected Object lookup(Object key) {
        return deserialize(doLookup(cacheMetadata.getKeyPrefix(), key));
    }

    private byte[] doLookup(byte[] prefix, Object key) {

        return (byte[]) redisOperations.execute(new AbstractRedisCacheCallback<byte[]>(
                new BinaryRedisCacheElement(new RedisCacheElementX(prefix, key, null)
                        .withKeySerializer(redisOperations.getKeySerializer()), cacheValueAccessor), cacheMetadata) {

            @Override
            public byte[] doInRedis(BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {
                return connection.get(element.getKeyBytes());
            }
        });
    }

    private Object deserialize(byte[] bytes) {
        return bytes == null ? null : cacheValueAccessor.deserializeIfNecessary(bytes);
    }

    // XXX: 2019-09-03  thinking
    protected byte[] getRedisCacheKey(Object key) {
        return getRedisCacheKey(cacheMetadata.getKeyPrefix(), key);
    }

    protected byte[] getRedisCacheKey(byte[] prefix, Object key) {
        return RedisUtils.getKeyBytes(redisOperations, prefix, (String) key);
    }


    /**
     * {@link Callable} to transform a value obtained from another {@link Callable} to its store value.
     *
     * @author Mark Paluch
     * @see #toStoreValue(Object)
     * @since 1.8
     */
    private class StoreTranslatingCallable implements Callable<Object> {

        private Callable<?> valueLoader;

        public StoreTranslatingCallable(Callable<?> valueLoader) {
            this.valueLoader = valueLoader;
        }

        @Override
        public Object call() throws Exception {
            return toStoreValue(valueLoader.call());
        }
    }

    /**
     * Metadata required to maintain {@link RedisCache}. Keeps track of additional data structures required for processing
     * cache operations.
     *
     * @author Christoph Strobl
     * @since 1.5
     */
    static class RedisCacheMetadata {

        private final String cacheName;
        private final byte[] keyPrefix;
        private final byte[] setOfKnownKeys;
        private final byte[] cacheLockName;
        private long defaultExpiration = 0;

        /**
         * @param cacheName must not be {@literal null} or empty.
         * @param keyPrefix can be {@literal null}.
         */
        public RedisCacheMetadata(String cacheName, byte[] keyPrefix) {

            Assert.hasText(cacheName, "CacheName must not be null or empty!");
            this.cacheName = cacheName;
            this.keyPrefix = keyPrefix;

            StringRedisSerializer stringSerializer = new StringRedisSerializer();

            // name of the set holding the keys
            this.setOfKnownKeys = usesKeyPrefix() ? new byte[]{} : stringSerializer.serialize(cacheName + "~keys");
            this.cacheLockName = stringSerializer.serialize(cacheName + "~lock");
        }

        /**
         * @return true if the {@link RedisCache} uses a prefix for key ranges.
         */
        public boolean usesKeyPrefix() {
            return (keyPrefix != null && keyPrefix.length > 0);
        }

        /**
         * Get the binary representation of the key prefix.
         *
         * @return never {@literal null}.
         */
        public byte[] getKeyPrefix() {
            return this.keyPrefix;
        }

        /**
         * Get the binary representation of the key identifying the data structure used to maintain known keys.
         *
         * @return never {@literal null}.
         */
        public byte[] getSetOfKnownKeysKey() {
            return setOfKnownKeys;
        }

        /**
         * Get the binary representation of the key identifying the data structure used to lock the cache.
         *
         * @return never {@literal null}.
         */
        public byte[] getCacheLockKey() {
            return cacheLockName;
        }

        /**
         * Get the name of the cache.
         *
         * @return
         */
        public String getCacheName() {
            return cacheName;
        }

        /**
         * Set the default expiration time in seconds
         *
         * @param seconds
         */
        public void setDefaultExpiration(long seconds) {
            this.defaultExpiration = seconds;
        }

        /**
         * Get the default expiration time in seconds.
         *
         * @return
         */
        public long getDefaultExpiration() {
            return defaultExpiration;
        }

    }

    /**
     * @author Christoph Strobl
     * @since 1.5
     */
    static class CacheValueAccessor {

        @SuppressWarnings("rawtypes") //
        private final RedisSerializer valueSerializer;

        @SuppressWarnings("rawtypes")
        CacheValueAccessor(RedisSerializer valueRedisSerializer) {
            valueSerializer = valueRedisSerializer;
        }

        byte[] convertToBytesIfNecessary(Object value) {

            if (value == null) {
                return new byte[0];
            }

            if (valueSerializer == null && value instanceof byte[]) {
                return (byte[]) value;
            }

            return valueSerializer.serialize(value);
        }

        Object deserializeIfNecessary(byte[] value) {

            if (valueSerializer != null) {
                return valueSerializer.deserialize(value);
            }

            return value;
        }
    }

    /**
     * @author Christoph Strobl
     * @since 1.6
     */
    static class BinaryRedisCacheElement extends RedisCacheElementX {

        private byte[] keyBytes;
        private byte[] valueBytes;
        private RedisCacheElementX element;
        private boolean lazyLoad;
        private CacheValueAccessor accessor;

        public BinaryRedisCacheElement(RedisCacheElementX element, CacheValueAccessor accessor) {

            super(element.getPrefix(), element.getKeyElement(), element.get(), element.getKeySerializer());
            this.element = element;
            this.keyBytes = element.getKeyBytes();
            this.accessor = accessor;

            lazyLoad = element.get() instanceof Callable;
            this.valueBytes = lazyLoad ? null : accessor.convertToBytesIfNecessary(element.get());
        }

        @Override
        public byte[] getKeyBytes() {
            return keyBytes;
        }

        public long getTimeToLive() {
            return element.getTimeToLive();
        }

        public boolean hasKeyPrefix() {
            return element.hasKeyPrefix();
        }

        public boolean isEternal() {
            return element.isEternal();
        }

        public RedisCacheElementX expireAfter(long seconds) {
            return element.expireAfter(seconds);
        }

        @Override
        public byte[] get() {

            if (lazyLoad && valueBytes == null) {
                try {
                    valueBytes = accessor.convertToBytesIfNecessary(((Callable<?>) element.get()).call());
                } catch (Exception e) {
                    throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e.getMessage(), e);
                }
            }
            return valueBytes;
        }
    }

    /**
     * @param <T>
     * @author Christoph Strobl
     * @since 1.5
     */
    static abstract class AbstractRedisCacheCallback<T> implements RedisCallback<T> {

        private long WAIT_FOR_LOCK_TIMEOUT = 300;
        private final BinaryRedisCacheElement element;
        private final RedisCacheMetadata cacheMetadata;

        public AbstractRedisCacheCallback(BinaryRedisCacheElement element, RedisCacheMetadata metadata) {
            this.element = element;
            this.cacheMetadata = metadata;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.core.RedisCallback#doInRedis(org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public T doInRedis(RedisConnection connection) throws DataAccessException {
            waitForLock(connection);
            return doInRedis(element, connection);
        }

        public abstract T doInRedis(BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException;

        protected void processKeyExpiration(RedisCacheElementX element, RedisConnection connection) {
            if (!element.isEternal()) {
                connection.expire(element.getKeyBytes(), element.getTimeToLive());
            }
        }

        protected void maintainKnownKeys(RedisCacheElementX element, RedisConnection connection) {

            if (!element.hasKeyPrefix()) {

                connection.zAdd(cacheMetadata.getSetOfKnownKeysKey(), 0, element.getKeyBytes());

                if (!element.isEternal()) {
                    connection.expire(cacheMetadata.getSetOfKnownKeysKey(), element.getTimeToLive());
                }
            }
        }

        protected void cleanKnownKeys(RedisCacheElementX element, RedisConnection connection) {

            if (!element.hasKeyPrefix()) {
                connection.zRem(cacheMetadata.getSetOfKnownKeysKey(), element.getKeyBytes());
            }
        }

        protected boolean waitForLock(RedisConnection connection) {

            boolean retry;
            boolean foundLock = false;
            do {
                retry = false;
                if (connection.exists(cacheMetadata.getCacheLockKey())) {
                    foundLock = true;
                    try {
                        Thread.sleep(WAIT_FOR_LOCK_TIMEOUT);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    retry = true;
                }
            } while (retry);

            return foundLock;
        }

        protected void lock(RedisConnection connection) {
            waitForLock(connection);
            connection.set(cacheMetadata.getCacheLockKey(), "locked".getBytes());
        }

        protected void unlock(RedisConnection connection) {
            connection.del(cacheMetadata.getCacheLockKey());
        }
    }

    /**
     * @param <T>
     * @author Christoph Strobl
     * @since 1.5
     */
    static abstract class LockingRedisCacheCallback<T> implements RedisCallback<T> {

        private final RedisCacheMetadata metadata;

        public LockingRedisCacheCallback(RedisCacheMetadata metadata) {
            this.metadata = metadata;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.core.RedisCallback#doInRedis(org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public T doInRedis(RedisConnection connection) throws DataAccessException {

            if (connection.exists(metadata.getCacheLockKey())) {
                return null;
            }
            try {
                connection.set(metadata.getCacheLockKey(), metadata.getCacheLockKey());
                return doInLock(connection);
            } finally {
                connection.del(metadata.getCacheLockKey());
            }
        }

        public abstract T doInLock(RedisConnection connection);
    }

    /**
     * @author Christoph Strobl
     * @since 1.5
     */
    static class RedisCacheCleanByKeysCallback extends LockingRedisCacheCallback<Void> {

        private static final int PAGE_SIZE = 128;
        private final RedisCacheMetadata metadata;

        RedisCacheCleanByKeysCallback(RedisCacheMetadata metadata) {
            super(metadata);
            this.metadata = metadata;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.cache.RedisCache.LockingRedisCacheCallback#doInLock(org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public Void doInLock(RedisConnection connection) {

            int offset = 0;
            boolean finished = false;

            do {
                // need to paginate the keys
                Set<byte[]> keys = connection.zRange(metadata.getSetOfKnownKeysKey(), (offset) * PAGE_SIZE,
                        (offset + 1) * PAGE_SIZE - 1);
                finished = keys.size() < PAGE_SIZE;
                offset++;
                if (!keys.isEmpty()) {
                    connection.del(keys.toArray(new byte[keys.size()][]));
                }
            } while (!finished);

            connection.del(metadata.getSetOfKnownKeysKey());
            return null;
        }
    }

    /**
     * @author Christoph Strobl
     * @since 1.5
     */
    static class RedisCacheCleanByPrefixCallback extends LockingRedisCacheCallback<Void> {

        private static final byte[] REMOVE_KEYS_BY_PATTERN_LUA = new StringRedisSerializer().serialize(
                "local keys = redis.call('KEYS', ARGV[1]); local keysCount = table.getn(keys); if(keysCount > 0) then for _, key in ipairs(keys) do redis.call('del', key); end; end; return keysCount;");
        private static final byte[] WILD_CARD = new StringRedisSerializer().serialize("*");
        private final RedisCacheMetadata metadata;

        public RedisCacheCleanByPrefixCallback(RedisCacheMetadata metadata) {
            super(metadata);
            this.metadata = metadata;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.cache.RedisCache.LockingRedisCacheCallback#doInLock(org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public Void doInLock(RedisConnection connection) throws DataAccessException {

            byte[] prefixToUse = Arrays.copyOf(metadata.getKeyPrefix(), metadata.getKeyPrefix().length + WILD_CARD.length);
            System.arraycopy(WILD_CARD, 0, prefixToUse, metadata.getKeyPrefix().length, WILD_CARD.length);

            if (isClusterConnection(connection)) {

                // load keys to the client because currently Redis Cluster connections do not allow eval of lua scripts.
                Set<byte[]> keys = connection.keys(prefixToUse);
                if (!keys.isEmpty()) {
                    connection.del(keys.toArray(new byte[keys.size()][]));
                }
            } else {
                connection.eval(REMOVE_KEYS_BY_PATTERN_LUA, ReturnType.INTEGER, 0, prefixToUse);
            }

            return null;
        }
    }

    /**
     * @author Christoph Strobl
     * @since 1.5
     */
    static class RedisCacheEvictCallback extends AbstractRedisCacheCallback<Void> {

        public RedisCacheEvictCallback(BinaryRedisCacheElement element, RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisCacheCallback#doInRedis(org.springframework.data.redis.cache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public Void doInRedis(BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {

            connection.del(element.getKeyBytes());
            cleanKnownKeys(element, connection);
            return null;
        }
    }

    /**
     * @author Christoph Strobl
     * @since 1.5
     */
    static class RedisCachePutCallback extends AbstractRedisCacheCallback<Void> {

        public RedisCachePutCallback(BinaryRedisCacheElement element, RedisCacheMetadata metadata) {

            super(element, metadata);
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#doInRedis(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public Void doInRedis(BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {

            if (!isClusterConnection(connection)) {
                connection.multi();
            }

            if (element.get().length == 0) {
                connection.del(element.getKeyBytes());
            } else {
                connection.set(element.getKeyBytes(), element.get());

                processKeyExpiration(element, connection);
                maintainKnownKeys(element, connection);
            }

            if (!isClusterConnection(connection)) {
                connection.exec();
            }
            return null;
        }
    }

    /**
     * @author Christoph Strobl
     * @since 1.5
     */
    static class RedisCachePutIfAbsentCallback extends AbstractRedisCacheCallback<byte[]> {

        public RedisCachePutIfAbsentCallback(BinaryRedisCacheElement element, RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#doInRedis(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
         */
        @Override
        public byte[] doInRedis(BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {

            waitForLock(connection);

            byte[] keyBytes = element.getKeyBytes();
            byte[] value = element.get();

            if (!connection.setNX(keyBytes, value)) {
                return connection.get(keyBytes);
            }

            maintainKnownKeys(element, connection);
            processKeyExpiration(element, connection);

            return null;
        }
    }

    /**
     * @author Christoph Strobl
     * @since 1.7
     */
    static class RedisWriteThroughCallback extends AbstractRedisCacheCallback<byte[]> {

        public RedisWriteThroughCallback(BinaryRedisCacheElement element, RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        @Override
        public byte[] doInRedis(BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {

            try {

                lock(connection);

                try {

                    byte[] value = connection.get(element.getKeyBytes());

                    if (value != null) {
                        return value;
                    }

                    if (!isClusterConnection(connection)) {

                        connection.watch(element.getKeyBytes());
                        connection.multi();
                    }

                    value = element.get();

                    if (value.length == 0) {
                        connection.del(element.getKeyBytes());
                    } else {
                        connection.set(element.getKeyBytes(), value);
                        processKeyExpiration(element, connection);
                        maintainKnownKeys(element, connection);
                    }

                    if (!isClusterConnection(connection)) {
                        connection.exec();
                    }

                    return value;
                } catch (RuntimeException e) {
                    if (!isClusterConnection(connection)) {
                        connection.discard();
                    }
                    throw e;
                }
            } finally {
                unlock(connection);
            }
        }
    }

    ;

    /**
     * @author Christoph Strobl
     * @since 1.7 (TODO: remove when upgrading to spring 4.3)
     */
    private static enum CacheValueRetrievalExceptionFactory {

        INSTANCE;

        private static boolean isSpring43;

        static {
            isSpring43 = ClassUtils.isPresent("org.springframework.cache.Cache$ValueRetrievalException",
                    ClassUtils.getDefaultClassLoader());
        }

        public RuntimeException create(Object key, Callable<?> valueLoader, Throwable cause) {

            if (isSpring43) {
                try {
                    Class<?> execption = ClassUtils.forName("org.springframework.cache.Cache$ValueRetrievalException",
                            this.getClass().getClassLoader());
                    Constructor<?> c = ClassUtils.getConstructorIfAvailable(execption, Object.class, Callable.class,
                            Throwable.class);
                    return (RuntimeException) c.newInstance(key, valueLoader, cause);
                } catch (Exception ex) {
                    // ignore
                }
            }

            return new RedisSystemException(
                    String.format("Value for key '%s' could not be loaded using '%s'.", key, valueLoader), cause);
        }
    }

    private static boolean isClusterConnection(RedisConnection connection) {

        while (connection instanceof DecoratedRedisConnection) {
            connection = ((DecoratedRedisConnection) connection).getDelegate();
        }

        return connection instanceof RedisClusterConnection;
    }


}
