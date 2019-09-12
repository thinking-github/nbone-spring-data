package org.springframework.cache.annotation;

import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-10
 */
public class RedisHashKeyGenerator implements KeyGenerator {

    @Override
    public Object generate(Object target, Method method, Object... params) {

        //默认采用全部限定名
        String key = target.getClass().getName();

        RedisHashKey redisHashKey = null;

        if (params.length == 0) {
            return SimpleKey.EMPTY;
        }
        for (Object param : params) {
            if (param instanceof CacheKey) {
                redisHashKey = ((CacheKey) param).getCacheKey();
                Assert.notNull(redisHashKey, "getCacheKey method return value must not be null.thinking");
                redisHashKey.setTarget(target);
                redisHashKey.setMethod(method);
                break;
            }
        }
        if (redisHashKey == null) {
            throw new IllegalArgumentException("must implements interface: " + CacheKey.class.getName() + " set getCacheKey method");
        }

        return redisHashKey;
    }
}
