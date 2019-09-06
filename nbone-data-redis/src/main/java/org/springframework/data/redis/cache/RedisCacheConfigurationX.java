package org.springframework.data.redis.cache;

import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-03
 */
public class RedisCacheConfigurationX {

    private final Duration ttl;
    private final boolean cacheNullValues;
    private final String keyPrefix;
    private final boolean usePrefix;

    private final ConversionService conversionService;

    @SuppressWarnings("unchecked")
    private RedisCacheConfigurationX(Duration ttl, Boolean cacheNullValues, Boolean usePrefix, String keyPrefix,
                                     ConversionService conversionService) {

        this.ttl = ttl;
        this.cacheNullValues = cacheNullValues;
        this.usePrefix = usePrefix;
        this.keyPrefix = keyPrefix;
        this.conversionService = conversionService;
    }

    /**
     * Default {@link RedisCacheConfigurationX} using the following:
     * <dl>
     * <dt>key expiration</dt>
     * <dd>eternal</dd>
     * <dt>cache null values</dt>
     * <dd>yes</dd>
     * <dt>prefix cache keys</dt>
     * <dd>yes</dd>
     * <dt>default prefix</dt>
     * <dd>[the actual cache name]</dd>
     * <dt>key serializer</dt>
     * <dd>StringRedisSerializer.class</dd>
     * <dt>value serializer</dt>
     * <dd>JdkSerializationRedisSerializer.class</dd>
     * <dt>conversion service</dt>
     * <dd>{@link DefaultFormattingConversionService} with {@link #registerDefaultConverters(ConverterRegistry) default}
     * cache key converters</dd>
     * </dl>
     *
     * @return new {@link RedisCacheConfigurationX}.
     */
    public static RedisCacheConfigurationX defaultCacheConfig() {

        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();

        registerDefaultConverters(conversionService);

        return new RedisCacheConfigurationX(Duration.ZERO, true, true, null, conversionService);
    }

    /**
     * Set the ttl to apply for cache entries. Use {@link Duration#ZERO} to declare an eternal cache.
     *
     * @param ttl must not be {@literal null}.
     */
    public RedisCacheConfigurationX entryTtl(Duration ttl) {

        Assert.notNull(ttl, "TTL duration must not be null!");

        return new RedisCacheConfigurationX(ttl, cacheNullValues, usePrefix, keyPrefix, conversionService);
    }

    /**
     * Use the given prefix instead of the default one.
     *
     * @param prefix must not be {@literal null}.
     */
    public RedisCacheConfigurationX prefixKeysWith(String prefix) {

        Assert.notNull(prefix, "Prefix must not be null!");

        return new RedisCacheConfigurationX(ttl, cacheNullValues, true, prefix, conversionService);
    }

    /**
     * Disable caching {@literal null} values. <br />
     * <strong>NOTE</strong> any {@link org.springframework.cache.Cache#put(Object, Object)} operation involving
     * {@literal null} value will error. Nothing will be written to Redis, nothing will be removed. An already existing
     * key will still be there afterwards with the very same value as before.
     */
    public RedisCacheConfigurationX disableCachingNullValues() {
        return new RedisCacheConfigurationX(ttl, false, usePrefix, keyPrefix, conversionService);
    }

    /**
     * Disable using cache key prefixes. <br />
     * <strong>NOTE</strong>: {@link Cache#clear()} might result in unintended removal of {@literal key}s in Redis. Make
     * sure to use a dedicated Redis instance when disabling prefixes.
     */
    public RedisCacheConfigurationX disableKeyPrefix() {

        return new RedisCacheConfigurationX(ttl, cacheNullValues, false, keyPrefix, conversionService);
    }

    /**
     * Define the {@link ConversionService} used for cache key to {@link String} conversion.
     *
     * @param conversionService must not be {@literal null}.
     */
    public RedisCacheConfigurationX withConversionService(ConversionService conversionService) {

        Assert.notNull(conversionService, "ConversionService must not be null!");

        return new RedisCacheConfigurationX(ttl, cacheNullValues, usePrefix, keyPrefix, conversionService);
    }


    /**
     * @return never {@literal null}.
     */
    public Optional<String> getKeyPrefix() {
        return Optional.ofNullable(keyPrefix);
    }

    /**
     * @return {@literal true} if cache keys need to be prefixed with the {@link #getKeyPrefix()} if present or the
     * default which resolves to {@link Cache#getName()}.
     */
    public boolean usePrefix() {
        return usePrefix;
    }

    /**
     * @return {@literal true} if caching {@literal null} is allowed.
     */
    public boolean getAllowCacheNullValues() {
        return cacheNullValues;
    }


    /**
     * @return The expiration time (ttl) for cache entries. Never {@literal null}.
     */
    public Duration getTtl() {
        return ttl;
    }

    /**
     * @return The {@link ConversionService} used for cache key to {@link String} conversion. Never {@literal null}.
     */
    public ConversionService getConversionService() {
        return conversionService;
    }

    /**
     * Registers default cache key converters. The following converters get registered:
     * <ul>
     * <li>{@link String} to {@link byte byte[]} using UTF-8 encoding.</li>
     * <li>{@link SimpleKey} to {@link String}</li>
     *
     * @param registry must not be {@literal null}.
     */
    public static void registerDefaultConverters(ConverterRegistry registry) {

        Assert.notNull(registry, "ConverterRegistry must not be null!");

        registry.addConverter(String.class, byte[].class, source -> source.getBytes(StandardCharsets.UTF_8));
        registry.addConverter(SimpleKey.class, String.class, SimpleKey::toString);
    }
}
