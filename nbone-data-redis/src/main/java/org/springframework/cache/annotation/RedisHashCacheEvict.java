package org.springframework.cache.annotation;

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * Created by chenyicheng on 2019-09-08.
 *
 * @author chenyicheng
 * @version 1.0
 * @since 2019-09-08
 */
@CacheEvict
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface RedisHashCacheEvict {

    @AliasFor(annotation = CacheEvict.class)
    String[] value() default {};

    @AliasFor(annotation = CacheEvict.class)
    String[] cacheNames() default {};

    @AliasFor(annotation = CacheEvict.class)
    String key() default "#p0.cacheKey";

    @AliasFor(annotation = CacheEvict.class)
    String keyGenerator() default "";

    @AliasFor(annotation = CacheEvict.class)
    String cacheManager() default "redisHashManager";

    @AliasFor(annotation = CacheEvict.class)
    String cacheResolver() default "";

    @AliasFor(annotation = CacheEvict.class)
    String condition() default "";

    @AliasFor(annotation = CacheEvict.class)
    boolean allEntries() default false;

    @AliasFor(annotation = CacheEvict.class)
    boolean beforeInvocation() default false;

}
