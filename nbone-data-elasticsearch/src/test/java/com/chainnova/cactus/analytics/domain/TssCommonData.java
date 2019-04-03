package com.chainnova.cactus.analytics.domain;

import org.springframework.data.elasticsearch.annotations.Document;

/**
 *
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/8
 */
@Document(indexName = "salix_693fe72810_5f198cd100_23198cd098",type ="salix_693fe72810_5f198cd100_23198cd098" )
public class TssCommonData {

    private String key;
    private Object value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }


}
