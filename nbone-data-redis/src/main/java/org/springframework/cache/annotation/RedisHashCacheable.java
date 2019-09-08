package org.springframework.cache.annotation;

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019-09-08
 */
@Cacheable
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface RedisHashCacheable {

    @AliasFor(annotation = Cacheable.class)
    String[] value() default {};

    @AliasFor(annotation = Cacheable.class)
    String[] cacheNames() default {};

    @AliasFor(annotation = Cacheable.class)
    String key() default "#p0.cacheKey";

    @AliasFor(annotation = Cacheable.class)
    String keyGenerator() default "";

    @AliasFor(annotation = Cacheable.class)
    String cacheManager() default "redisHashManager";

    @AliasFor(annotation = Cacheable.class)
    String cacheResolver() default "";

    @AliasFor(annotation = Cacheable.class)
    String condition() default "";

    @AliasFor(annotation = Cacheable.class)
    String unless() default "";

    @AliasFor(annotation = Cacheable.class)
    boolean sync() default false;

}
