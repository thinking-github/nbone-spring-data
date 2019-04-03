package org.nbone.spring.data.elasticsearch.test;

import com.alibaba.fastjson.JSON;
import com.chainnova.cactus.analytics.domain.TssCommonData;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nbone.spring.data.elasticsearch.core.ElasticsearchTemplate;
import org.nbone.spring.data.elasticsearch.core.ElasticsearchUtils;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/10
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/spring/spring-context.xml")
public class ElasticsearchQueryTest {

    @Resource
    private ElasticsearchTemplate template;


    String index = "salix_693fe72810_5f198cd100_23198cd098";
    String type = "salix_693fe72810_5f198cd100_23198cd098";


    @Test
    public void getBookEntity() {

        GetQuery getQuery = new GetQuery();
        getQuery.setId("AWd-EddLxboxWPySyRHO");
        TssCommonData data = template.queryForObject(getQuery, TssCommonData.class);
        System.out.println(data);
    }

    @Test
    public void findAllEntity() {
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withQuery(matchAllQuery())
                .withIndices(index)
                .withTypes(type)
                .withFields("value.memberCode", "value.tenantCode")
                .withHighlightFields(new HighlightBuilder.Field("value.tenantCode"))
                .withPageable(PageRequest.of(0, 10))
                .build();
        Page<TssCommonData> samples = template.queryForPage(searchQuery1, TssCommonData.class);
        Page<TssCommonData> samples1 = template.queryForPage(searchQuery1, TssCommonData.class);
        Page<TssCommonData> samples2 = template.queryForPage(searchQuery1, TssCommonData.class);
        Page<TssCommonData> samples3 = template.queryForPage(searchQuery1, TssCommonData.class);
        System.out.println(samples);
    }

    /**
     * 结构化查询
     * SELECT * FROM salix_693fe72810_5f198cd100_23198cd098  WHERE  value.tenantCode = '693fe72810' and value.memberCode = '5f198cd100' and value.projectCode = '23198cd098' and value.itemCode = '956a6ac568'
     */
    @Test
    public void findEntity() {
        //filter
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withQuery(matchAllQuery())
                .withFilter(boolQuery().must(termQuery("value.tenantCode", "693fe72810"))
                        .must(termQuery("value.memberCode", "5f198cd100"))
                        .must(termQuery("value.projectCode", "23198cd098"))
                        .must(termQuery("value.itemCode", "956a6ac568"))
                        .should(termQuery("value.userSex", "男"))
                )

                .withIndices(index)
                .withTypes(type)
                //.withFields("message")
                .withPageable(PageRequest.of(0, 10))
                .build();


        //query
        SearchQuery searchQuery2 = new NativeSearchQueryBuilder()
                .withQuery(boolQuery().must(termQuery("value.tenantCode", "693fe72810"))
                        .must(termQuery("value.memberCode", "5f198cd100"))
                        .must(termQuery("value.projectCode", "23198cd098"))
                        .must(termQuery("value.itemCode", "956a6ac568"))
                        .must(rangeQuery("value.traceTimestamp").from(1544008355508L).to(1545019014596L))
                )
                .withIndices(index)
                .withTypes(type)
                //.withFields("message")
                .withPageable(PageRequest.of(0, 10))
                .build();

        SearchQuery searchQuery3 = new NativeSearchQueryBuilder()
                .withQuery(termsQuery("value.userSex", "男", "女"))
                .withIndices(index)
                .withTypes(type)
                //.withFields("message")
                .withPageable(PageRequest.of(0, 10))
                .build();

        Page<TssCommonData> samples1 = template.queryForPage(searchQuery1, TssCommonData.class);
        Page<TssCommonData> samples2 = template.queryForPage(searchQuery2, TssCommonData.class);
        Page<TssCommonData> samples3 = template.queryForPage(searchQuery3, TssCommonData.class);

        System.out.println(samples1);
    }

    /**
     * 非空字段查询,可以匹配如下：
     * { "user": "jane" }
     * { "user": "" }
     * { "user": "-" }
     * { "user": ["jane"] }
     * { "user": ["jane", null ] }
     * <p>
     * 如下不匹配：
     * { "user": null }
     * { "user": [] }
     * { "user": [null] }
     * { "foo":  "bar" }
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html#query-dsl-exists-query
     */
    @Test
    public void findExistEntity() {
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withQuery(existsQuery("value.keyData.checkName"))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();
        Page<TssCommonData> samples = template.queryForPage(searchQuery1, TssCommonData.class);
        System.out.println(samples);
    }

    /**
     * 部分匹配
     */
    @Test
    public void findLikeEntity() {
        //通配符匹配
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withQuery(wildcardQuery("value.keyData.checkName", "微量*"))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();


        //前缀匹配
        SearchQuery searchQuery2 = new NativeSearchQueryBuilder()
                .withQuery(prefixQuery("value.keyData.checkName", "微量"))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();


        //正则表达式匹配
        SearchQuery searchQuery3 = new NativeSearchQueryBuilder()
                .withQuery(regexpQuery("value.keyData.checkName", "微量"))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();


        Page<TssCommonData> samples1 = template.queryForPage(searchQuery1, TssCommonData.class);
        Page<TssCommonData> samples2 = template.queryForPage(searchQuery2, TssCommonData.class);
        Page<TssCommonData> samples3 = template.queryForPage(searchQuery3, TssCommonData.class);
        System.out.println(samples1);
    }

    /**
     * 模糊(不常用)
     */
    @Test
    public void findfuzzyEntity() {

        //拼写错误模糊
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(fuzzyQuery("value.phaseCode", "f3577eaf").fuzziness(Fuzziness.AUTO))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();


        Page<TssCommonData> samples = template.queryForPage(searchQuery, TssCommonData.class);
        System.out.println(samples);
    }

    /**
     * 全文检索查询
     */
    @Test
    public void findMatchEntity() {
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withQuery(matchQuery("value.keyData.checkName", "微量").operator(Operator.AND))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();

        SearchQuery searchQuery2 = new NativeSearchQueryBuilder()
                .withQuery(multiMatchQuery("航空食品检测中心", "value.keyData.checkName", "value.keyData.checkArch"))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();


        Page<TssCommonData> samples1 = template.queryForPage(searchQuery1, TssCommonData.class);
        Page<TssCommonData> samples2 = template.queryForPage(searchQuery2, TssCommonData.class);
        System.out.println(samples1);
    }


    /**
     *   query_string
     *  "query": "(content:this OR name:this) AND (content:that OR name:that)"
     */
    @Test
    public void findQueryStringEntity() {
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withQuery(queryStringQuery("value.keyData.checkName:营养成分"))
                .withIndices(index)
                .withTypes(type)
                .withPageable(PageRequest.of(0, 10))
                .build();


        Page<TssCommonData> samples1 = template.queryForPage(searchQuery1, TssCommonData.class);
        System.out.println(samples1);
    }


    /**
     * 本地发送sql至服务器查询  (服务器实现sql解析功能)
     * @throws IOException
     */
    @Test
    public void findEntityBysSql() throws IOException {
        String url = "http://120.92.109.71:7200/_sql";
        String sql = "SELECT * FROM salix_693fe72810_5f198cd100_23198cd098  WHERE value.tenantCode = '693fe72810' and value.memberCode = '5f198cd100' and value.projectCode = '23198cd098' and value.itemCode = '956a6ac568' ";

        String sql1 = "SELECT * FROM salix_693fe72810_5f198cd100_23198cd098  limit 3";

        Page<TssCommonData> page = template.queryForPageWithSql(url, sql1, TssCommonData.class);
        template.queryWithSql(url, sql);

        Page page1 = template.queryForPageWithSql(url, sql1);
        System.out.println(page);
        System.out.println(JSON.toJSONString(page));
        System.out.println("000");

    }

    /**
     *  本地解析sql成QDL， 发送QDL至服务器(本地实现sql解析功能)
     * @throws IOException
     * @throws SQLFeatureNotSupportedException
     * @throws SqlParseException
     */
    @Test
    public void findEntityByLocalSql() throws IOException {
        String sql = "SELECT * FROM salix_693fe72810_5f198cd100_23198cd098  WHERE value.tenantCode = '693fe72810' and value.memberCode = '5f198cd100' and value.projectCode = '23198cd098' and value.itemCode = '956a6ac568' ";

        String sql1 = "SELECT * FROM salix_693fe72810_5f198cd100_23198cd098  limit 3";

        Page<TssCommonData> page = template.getSqlPlugin().queryForPage(sql1, TssCommonData.class);

        Page<Map> pageMap1 = template.getSqlPlugin().queryForPage(sql1, Map.class);
        Page<Map> pageMap2 =  template.getSqlPlugin().queryForPage(sql1);

        System.out.println(page);
        System.out.println(JSON.toJSONString(page));
        System.out.println("000");

    }


    public static void main(String[] args) throws IOException {
        String responseJson = "";
        SearchResponse response = ElasticsearchUtils.getSearchResponseFromJsonString(responseJson);

        System.out.println(response);

    }


}
