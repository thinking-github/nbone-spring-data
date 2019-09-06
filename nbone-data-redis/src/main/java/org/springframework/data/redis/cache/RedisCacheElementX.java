package org.springframework.data.redis.cache;

import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.data.redis.serializer.RedisSerializer;

import static org.springframework.util.Assert.notNull;

/**
 * Redis key:value cacheElement
 *
 * @author thinking
 * @version 1.0
 * @since 2019-09-03
 */
@SuppressWarnings("unused")
public class RedisCacheElementX extends SimpleValueWrapper {

    private RedisSerializer<String> stringSerializer = RedisUtils.DEFAULT_STRING_SERIALIZER;
    private RedisSerializer keySerializer;

    private String keyElement;
    private byte[] prefix;

    private long timeToLive;


    /**
     * @param prefix     前缀 可为空
     * @param keyElement the key to be used for storing value in {@link RedisCache}. Must not be {@literal null}.
     * @param value
     */
    public RedisCacheElementX(byte[] prefix, Object keyElement, Object value) {
        super(value);
        notNull(keyElement, "CacheKey must not be null!");
        String key = null;
        if (keyElement instanceof String) {
            key = (String) keyElement;
        }

        this.prefix = prefix;
        this.keyElement = key;
    }
    public RedisCacheElementX(byte[] prefix, Object keyElement, Object value,RedisSerializer keySerializer) {
       this(prefix,keyElement,value);
       this.withKeySerializer(keySerializer);
    }

    /**
     * Get the binary key representation.
     *
     * @return
     */
    public byte[] getKeyBytes() {
        if (!hasPrefix()) {
            return keySerializer.serialize(keyElement);
        }
        // string
        String keyUsed = stringSerializer.deserialize(prefix) + keyElement;

        return keySerializer.serialize(keyUsed);
    }

    /**
     * @return
     */
    public <T> T getKey() {
        return (T) keySerializer.deserialize(getKeyBytes());
    }

    /**
     * Set the elements time to live. Use {@literal zero} to store eternally.
     *
     * @param timeToLive
     */
    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
    }

    /**
     * @return
     */
    public long getTimeToLive() {
        return timeToLive;
    }

    /**
     * @return true in case {@link RedisCacheKey} is prefixed.
     */
    public boolean hasKeyPrefix() {
        return hasPrefix();
    }

    /**
     * @return true if timeToLive is 0
     */
    public boolean isEternal() {
        return 0 == timeToLive;
    }

    /**
     * Expire the element after given seconds.
     *
     * @param seconds
     * @return
     */
    public RedisCacheElementX expireAfter(long seconds) {

        setTimeToLive(seconds);
        return this;
    }


    // XXX: 2019-09-03  thinking
    public String getKeyElement() {
        return keyElement;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    public boolean hasPrefix() {
        return (prefix != null && prefix.length > 0);
    }

    public RedisCacheElementX prefix(byte[] prefix) {
        this.prefix = prefix;
        return this;
    }

    public RedisSerializer getKeySerializer() {
        return keySerializer;
    }

    public RedisCacheElementX withKeySerializer(RedisSerializer serializer) {
        this.keySerializer = serializer;
        return this;
    }

}

