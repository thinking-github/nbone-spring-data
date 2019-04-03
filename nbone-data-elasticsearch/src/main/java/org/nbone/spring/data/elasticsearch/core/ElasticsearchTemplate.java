package org.nbone.spring.data.elasticsearch.core;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.ResultsMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/9
 */
@SuppressWarnings("unchecked")
public class ElasticsearchTemplate extends org.springframework.data.elasticsearch.core.ElasticsearchTemplate {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTemplate.class);
    private Client client;


    private String searchTimeout;

    /**
     * rest remote sql查询
     */
    private String sqlServerPath;

    /**
     * 是否支持本地sql查询
     */
    private boolean supportSql;

    private SqlPlugin sqlPlugin;


    public ElasticsearchTemplate(Client client) {
        super(client);
        this.client = client;
    }

    public ElasticsearchTemplate(Client client, EntityMapper entityMapper) {
        super(client, entityMapper);
        this.client = client;
    }

    public ElasticsearchTemplate(Client client, ElasticsearchConverter elasticsearchConverter, EntityMapper entityMapper) {
        super(client, elasticsearchConverter, entityMapper);
        this.client = client;
    }

    public ElasticsearchTemplate(Client client, ResultsMapper resultsMapper) {
        super(client, resultsMapper);
        this.client = client;
    }

    public ElasticsearchTemplate(Client client, ElasticsearchConverter elasticsearchConverter) {
        super(client, elasticsearchConverter);
        this.client = client;
    }

    public ElasticsearchTemplate(Client client, ElasticsearchConverter elasticsearchConverter, ResultsMapper resultsMapper) {
        super(client, elasticsearchConverter, resultsMapper);
        this.client = client;
    }


    public void initSqlPlugin() {
        if (supportSql) {
            sqlPlugin = new SqlPlugin(client);
        }

    }

    @PostConstruct
    protected void init() {
        System.out.println("ElasticsearchTemplate init");
        initSqlPlugin();
    }


    @Override
    public void setSearchTimeout(String searchTimeout) {
        this.searchTimeout = searchTimeout;
        super.setSearchTimeout(searchTimeout);
    }

    public String getSqlServerPath() {
        return sqlServerPath;
    }

    public void setSqlServerPath(String sqlServerPath) {
        this.sqlServerPath = sqlServerPath;
    }

    public boolean isSupportSql() {
        return supportSql;
    }

    public void setSupportSql(boolean supportSql) {
        this.supportSql = supportSql;
    }

    public SqlPlugin getSqlPlugin() {
        return sqlPlugin;
    }

    // ElasticsearchTemplate  ElasticsearchOperations

    /**
     * 根据sql查询 返回结果集的内容(hits.hits[i]._source)映射对象
     *
     * @param url
     * @param sql
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T> AggregatedPage<T> queryForPageWithSql(String url, String sql, Class<T> clazz) throws IOException {

        sql = checkLimit(sql, null);
        RestTemplate restTemplate = HttpClient.getRestTemplate();
        HttpEntity<String> httpEntity = new HttpEntity<>(sql, HttpClient.getHeader());
        String responseJson = restTemplate.postForObject(url, httpEntity, String.class);

        SearchResponse response = ElasticsearchUtils.getSearchResponseFromJsonString(responseJson);

        ResultsMapper mapper = this.getResultsMapper();
        return mapper.mapResults(response, clazz, null);
    }

    public <T> AggregatedPage<T> queryForPageWithSql(String sql, Class<T> clazz) throws IOException {
        return queryForPageWithSql(sqlServerPath, sql, clazz);
    }


    /**
     * 根据sql查询 返回原生态Elasticsearch结果集映射成 map
     *
     * @param url
     * @param sql
     * @return
     * @throws IOException
     */
    public Map<String, Object> queryWithSql(String url, String sql) {

        RestTemplate restTemplate = HttpClient.getRestTemplate();
        HttpEntity<String> httpEntity = new HttpEntity<>(sql, HttpClient.getHeader());

        Map<String, Object> map = restTemplate.postForObject(url, httpEntity, Map.class);
        return map;
    }

    public Map<String, Object> queryWithSql(String sql) {
        return queryWithSql(sqlServerPath, sql);
    }

    public Page<List<Map<String, Object>>> queryForPageWithSql(String url, String sql) {

        sql = checkLimit(sql, null);
        Map<String, Object> map = queryWithSql(url, sql);

        if (map != null && map.get(ElasticsearchConsts.HITS) != null) {
            Map hitsMap = (Map) map.get(ElasticsearchConsts.HITS);
            Long total = Long.valueOf(hitsMap.get(ElasticsearchConsts.TOTAL) + "");
            List<Map<String, Object>> docs = (List<Map<String, Object>>) hitsMap.get(ElasticsearchConsts.HITS);

            List sources = docs.stream().map(hit -> hit.get(ElasticsearchConsts.SOURCE)).collect(Collectors.toList());
            Page<List<Map<String, Object>>> page = new PageImpl(sources, Pageable.unpaged(), total);

            return page;

        }

        return new PageImpl<List<Map<String, Object>>>(new ArrayList<>());

    }


    //-----sql local 解析查询------
    public class SqlPlugin {

        private SearchDao searchDao;

        public SqlPlugin(Client client) {
            this.searchDao = new SearchDao(client);
        }

        /**
         * 解析sql 成为QDL , 执行查询操作
         *
         * @param query
         * @return
         * @throws SqlParseException
         * @throws SQLFeatureNotSupportedException
         */
        public SearchResponse getSearchResponse(String query)  {
            //SearchDao searchDao = getSearchDao();
            SqlElasticSearchRequestBuilder select = null;
            try {
                select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();

            } catch (SqlParseException | SQLFeatureNotSupportedException e) {
                logger.error(e.getMessage(),e);
            }
            return ((SearchResponse) select.get());
        }


        /**
         * 查询结果映射成分页map
         * @param sql
         * @return
         */
        public AggregatedPage<Map> queryForPage(String sql) {
            SearchResponse response = getSearchResponse(sql);
            long totalHits = response.getHits().getTotalHits();
            List<Map> results = new ArrayList<>();
            for (SearchHit hit : response.getHits()) {
                if (hit != null) {
                    results.add(hit.getSource());
                }
            }
            return new AggregatedPageImpl<Map>(results, null, totalHits, response.getAggregations(), response.getScrollId());
        }

        /**
         * @param sql
         * @param clazz class 可以是 entity or map
         * @param <T>
         * @return
         */
        public <T> AggregatedPage<T> queryForPage(String sql, Class<T> clazz) {
            SearchResponse response = getSearchResponse(sql);
            ResultsMapper mapper = getResultsMapper();
            return mapper.mapResults(response, clazz, null);
        }


    }


    // private method
    private String checkLimit(String sql, Integer limit) {
        Assert.notNull(sql, "sql must not null.");
        if (limit == null) {
            limit = 10;
        }
        if (!sql.contains(ElasticsearchConsts.LIMIT)) {

            return sql + " " + ElasticsearchConsts.LIMIT + " " + limit;
        }

        return sql;
    }


    // static class
    public static class HttpClient {

        private static HttpHeaders header = new HttpHeaders();
        private static RestTemplate restTemplate = new RestTemplate();

        static {
            header.add(HttpHeaders.ACCEPT, "application/json, text/plain, */*");
            header.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);
            System.out.println("ElasticsearchTemplate.HttpClient init.thinking.");
        }

        public static RestTemplate getRestTemplate() {
            return restTemplate;
        }

        public static HttpHeaders getHeader() {
            return header;
        }
    }


}
