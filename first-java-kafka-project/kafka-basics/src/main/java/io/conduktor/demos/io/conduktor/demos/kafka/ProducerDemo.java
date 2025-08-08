package io.conduktor.demos.io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am kafka producer");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:19092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // Create the Producer Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java", "Hello World");

        // Send data
        producer.send(producerRecord);

        // Flush and close the producer
        producer.flush();
        producer.close();   // Also runs the flush, the flush above is not needed
    }
}
