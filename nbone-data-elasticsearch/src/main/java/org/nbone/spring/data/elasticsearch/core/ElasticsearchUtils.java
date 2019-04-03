package org.nbone.spring.data.elasticsearch.core;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;

/**
 *
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/10
 */
public class ElasticsearchUtils {


    /**
     * Elasticsearch  查询结果集JSON格式转化成 SearchResponse
     * @param responseJson
     * @return
     * @throws IOException
     */
    public static SearchResponse getSearchResponseFromJsonString(String responseJson) throws IOException{

        //JsonParser parser = new JsonFactory().createParser(responseJson);
        //JsonXContentParser xContentParser = new JsonXContentParser(NamedXContentRegistry.EMPTY, parser);


        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, responseJson);

        SearchResponse response = SearchResponse.fromXContent(xContentParser);

        return  response;
    }


    public static  String  getSearchResponse(String responseJson) {



        return  null;
    }
}
