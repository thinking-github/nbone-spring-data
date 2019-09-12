package org.springframework.cache.annotation;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-10
 */
public interface CacheKey {

    RedisHashKey getCacheKey();

}