package org.springframework.data.redis.cache;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Arrays;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-03
 */
public class RedisUtils {

    public static final RedisSerializer<String> DEFAULT_STRING_SERIALIZER = new StringRedisSerializer();

    public static byte[] getKeyBytes(byte[] prefix, String keyElement) {
        return getKeyBytes(DEFAULT_STRING_SERIALIZER, prefix, keyElement);
    }

    public static byte[] getKeyBytes(RedisOperations redisOperations, byte[] prefix, String keyElement) {
        RedisSerializer keySerializer = null;
        if (redisOperations instanceof RedisTemplate) {
            keySerializer = ((RedisTemplate) redisOperations).getStringSerializer();
        }
        return getKeyBytes(keySerializer, prefix, keyElement);

    }

    public static byte[] getKeyBytes(RedisSerializer keySerializer, byte[] prefix, String keyElement) {
        if (keySerializer == null) {
            keySerializer = DEFAULT_STRING_SERIALIZER;
        }
        byte[] rawKey = keySerializer.serialize(keyElement);
        if (!hasPrefix(prefix)) {
            return rawKey;
        }

        byte[] prefixedKey = Arrays.copyOf(prefix, prefix.length + rawKey.length);
        System.arraycopy(rawKey, 0, prefixedKey, prefix.length, rawKey.length);

        return prefixedKey;
    }


    public static boolean hasPrefix(byte[] prefix) {
        return (prefix != null && prefix.length > 0);
    }
}
