package org.springframework.cache.annotation;

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019-09-08
 */
@CachePut
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface RedisHashCachePut {

    @AliasFor(annotation = CachePut.class)
    String[] value() default {};

    @AliasFor(annotation = CachePut.class)
    String[] cacheNames() default {};

    @AliasFor(annotation = CachePut.class)
    String key() default "#p0.cacheKey";

    @AliasFor(annotation = CachePut.class)
    String keyGenerator() default "";

    @AliasFor(annotation = CachePut.class)
    String cacheManager() default "redisHashManager";

    @AliasFor(annotation = CachePut.class)
    String cacheResolver() default "";

    @AliasFor(annotation = CachePut.class)
    String condition() default "";

    @AliasFor(annotation = CachePut.class)
    String unless() default "";

}
