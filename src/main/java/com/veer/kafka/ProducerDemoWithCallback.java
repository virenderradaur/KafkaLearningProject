package com.veer.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerDemoWithCallback {
    public static final String bootstrapServer = "localhost:9092";
    public static final String topicName = "my_topic";

    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        log.debug("Start of main method");
        // Producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Construct Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        //send  with get()- synchronous  // send without get()- asynchronous
        // Never user get() - synchronous in production
        log.debug("Sending record...");
        for (int i = 0; i < 10; i++) {
            final String message = "Hello World " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, message);
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("Message:" + message + "\nSent to Topic :" + recordMetadata.topic() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "Timestamp :" + recordMetadata.timestamp());
                    } else {
                        log.error("Got Error :" + e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        log.debug("Closing ...!!!");
        kafkaProducer.close(10L, TimeUnit.MILLISECONDS);
    }
}
