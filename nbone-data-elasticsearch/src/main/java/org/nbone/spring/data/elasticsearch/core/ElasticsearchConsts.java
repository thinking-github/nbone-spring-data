package org.nbone.spring.data.elasticsearch.core;

/**
 *  <pre>
 {
 "took":36,
 "timed_out":false,
 "_shards":{
 "total":5,
 "successful":5,
 "failed":0 },
 "hits":{
 "total":126,
 "max_score":3.1749263,
 "hits":[{
 "_index":"leveldb",
 "_type":"test",
 "_id":"AWFF70Lzjr2NVyARxAQI",
 "_score":3.1749263,
 "_source":{
 "active":1,
 "area":76.5,
 "branchCompany":"宝泉岭分公司",
 "createTime":1504505410000,
 "fieldCode":"010210-2D",
 "id":16,
 "latitude":47.38702892,
 "longitude":130.50805498,
 "managementArea":"双峰山管理区",
 "name":"卢忠军",
 "organizationId":1000,
 "parentCompany":"黑龙江北大荒农业股份有限公司",
 "type":0,
 "updateTime":1504505410000,
 "workStation":"第二作业站"}
 },
 {"_index":"leveldb",
 "_type":"test",
 "_id":"AWFF70M1jr2NVyARxAQM",
 "_score":3.1749263,
 "_source":{
 "active":1,
 "area":103.5,
 "branchCompany":"宝泉岭分公司",
 "createTime":1504505410000,
 "fieldCode":"01023-2",
 "id":2,
 "latitude":47.39807687,
 "longitude":130.49538899,
 "managementArea":"双峰山管理区",
 "name":"毕志",
 "organizationId":1000,
 "parentCompany":"黑龙江北大荒农业股份有限公司",
 "type":0,
 "updateTime":1504505410000,
 "workStation":"第二作业站"}
 }
 ]}}
 * </pre>
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/11
 */
public interface ElasticsearchConsts {

    // result
    public static final String HITS = "hits";
    public static final String TOTAL = "total";
    public static final String MAX_SCORE = "max_score";

    public static final String INDEX = "_index";
    public static final String TYPE = "_type";
    public static final String ID = "_id";
    public static final String SCORE = "_score";
    public static final String SOURCE = "_source";


    //query
    public static final String ES_SQL = "_sql";
    public static final String ES_SEARCH = "_search";
    public static final String ES_COUNT = "_count";
    public static final String ES_DELETE_BY_QUERY = "_delete_by_query";
    public static final String ES_QUERY = "_query";
    public static final String ES_MAPPING = "_mapping";


    public static final String LIMIT = "limit";







}
