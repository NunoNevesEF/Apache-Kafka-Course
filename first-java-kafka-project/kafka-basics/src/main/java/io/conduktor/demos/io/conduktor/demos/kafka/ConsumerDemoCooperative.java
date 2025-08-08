package io.conduktor.demos.io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
    private static final String groupdId = "My java application";

    public static void main(String[] args) {
        log.info("I am kafka consumer");

        String topic = "demo_java";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:19092");
        // Create consumer configs

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupdId);

        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        // Create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // Get a reference for the current Thread
        final Thread mainThread = Thread.currentThread();

        // Adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown");
                consumer.wakeup();

                // join the mainThread to allow the execution of the code in the mainThread

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data
            while(true) {
                ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String,String> record: records) {
                    log.info("key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch ( WakeupException e) {
            log.info("Consumer is shutting down");
        } catch (Exception e) {
            log.info("Unexpected Exception");
        } finally {
            consumer.close();
            log.info("Consumer was closed");
        }


    }
}
