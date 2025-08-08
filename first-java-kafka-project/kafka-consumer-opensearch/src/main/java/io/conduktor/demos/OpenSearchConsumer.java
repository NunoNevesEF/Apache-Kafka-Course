package io.conduktor.demos;

import com.google.gson.JsonParser;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final String groupdId = "Consumer OpenSearch Demo";

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getScheme(), connUri.getHost(), connUri.getPort())));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(new AuthScope(null, -1), new UsernamePasswordCredentials(auth[0], auth[1].toCharArray()));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getScheme(), connUri.getHost(), connUri.getPort()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy()))
            );
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer createKafkaConsumer() {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:19092");
        // Create consumer configs

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupdId);
        properties.setProperty("auto.offset.reset", "latest");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        return JsonParser
                .parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // Create open search Client

        RestHighLevelClient openSearchClient = createOpenSearchClient();

        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        consumer.subscribe(Collections.singleton("wikimedia.recentchanges"));

        while(true) {

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(3000));

            int recordCount = records.count();

            BulkRequest bulkRequest = new BulkRequest();

            log.info("Received " + recordCount + " records");

            for (ConsumerRecord<String,String> record: records) {
                try {
                    String id = extractId(record.value());

                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    //IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    bulkRequest.add(indexRequest);
                } catch (Exception e ) {

                }
            }

            if( bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                //in case your not commiting the offsets auto
                //consumer.commitAsync();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
