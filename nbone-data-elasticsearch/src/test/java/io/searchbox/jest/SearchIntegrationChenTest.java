package io.searchbox.jest;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.Ping;
import io.searchbox.core.Search;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/18
 */

public class SearchIntegrationChenTest {

    private static final String INDEX = "salix_693fe72810_5f198cd100_23198cd098";
    private static final String TYPE = "salix_693fe72810_5f198cd100_23198cd098";


    protected final JestClientFactory factory = new JestClientFactory();
    protected JestHttpClient client;

    @Before
    public void setUp() throws Exception {
        factory.setHttpClientConfig(
                new HttpClientConfig
                        // .Builder("http://localhost:" + getPort())
                        .Builder("http://120.92.109.71:7200")
                        .readTimeout(10000).discoveryEnabled(true)
                        .multiThreaded(true).build()
        );
        client = (JestHttpClient) factory.getObject();
    }

    @Test
    public void searchAll() throws IOException {
        try {
            Thread.sleep(100000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        JestResult result = client.execute(new Search.Builder("").addIndex(INDEX).addType(TYPE).build());


        System.out.println(result);
    }


    @Test
    public void ping() throws IOException {
        Ping ping = new Ping.Builder().build();
        JestResult result = client.execute(ping);


        System.out.println(result.getJsonString());
    }

}
