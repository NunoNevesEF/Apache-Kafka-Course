package io.conduktor.demos.io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am kafka producer");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:19092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for ( int j = 0 ; j < 2 ; j++) {

            for ( int i = 0; i<10 ; i++) {
                // Create the Producer Record
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World " + i;

                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);

                // Send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if ( e == null) {
                            // Data was sent
                            log.info("Received new metadata");
                        } else {
                            // Exception thrown
                        }
                    }
                });

            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Flush and close the producer
        producer.flush();
        producer.close();   // Also runs the flush, the flush above is not needed
    }
}
