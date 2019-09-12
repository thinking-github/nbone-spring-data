package org.springframework.cache.annotation;

import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author thinking
 * @version 1.0
 * @since 2019-09-10
 */
public class RedisHashKey implements Serializable {

    private static final long serialVersionUID = 4239191728638053366L;


    private Object target;
    /**
     * 执行目标方法
     */
    private Method method;
    /**
     * redis hash 第一级key
     */
    private Object key;
    /**
     * hashMap key
     */
    private Object hashKey;

    private Collection<Object> hashKeys;

    /**
     * 是否含有上下文 即含有 target,method 描述
     */
    private boolean hasContext;


    public RedisHashKey(Object target, Method method, Object key, Object hashKey, Collection<Object> hashKeys) {
        this.target = target;
        this.method = method;
        this.key = key;
        this.hashKey = hashKey;
        this.hashKeys = hashKeys;
        this.hasContext = method != null ? true : false;
    }

    public RedisHashKey(String compositeKey) {
        String[] keys = StringUtils.commaDelimitedListToStringArray(compositeKey);
        if (keys.length == 1) {
            this.key = keys[0];
            this.hashKey = null;
        } else if (keys.length == 2) {
            this.key = keys[0];
            this.hashKey = keys[1];
        } else if (keys.length > 2) {
            this.key = keys[0];
            this.hashKeys = new HashSet<>();
            for (int i = 1; i < keys.length; i++) {
                hashKeys.add(keys[i]);
            }
        }

        this.hasContext = method != null ? true : false;
    }

    public RedisHashKey(Object key, Object hashKey) {
        this(null, null, key, hashKey, null);
    }

    public Object getTarget() {
        return target;
    }

    public void setTarget(Object target) {
        this.target = target;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public Class<?> getReturnType() {
        if (method != null) {
            return method.getReturnType();
        }
        return null;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getHashKey() {
        return hashKey;
    }

    public void setHashKey(Object hashKey) {
        this.hashKey = hashKey;
    }

    public Collection<?> getHashKeys() {
        return hashKeys;
    }

    public void setHashKeys(Collection<Object> hashKeys) {
        this.hashKeys = hashKeys;
    }

    public boolean isHasContext() {
        return hasContext;
    }

    public boolean isReturnTypeList() {
        return method != null && List.class.isAssignableFrom(getReturnType());
    }

    public boolean isReturnTypeMap() {
        return method != null && Map.class.isAssignableFrom(getReturnType());
    }

    public boolean isReturnTypeArray() {
        return method != null && Object[].class.isAssignableFrom(getReturnType());
    }
}
