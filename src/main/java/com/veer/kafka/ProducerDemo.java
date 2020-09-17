package com.veer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerDemo {
    public static final String bootstrapServer = "localhost:9092";
    public static final String topicName = "my_topic";
    public static void main(String[] args) {
        System.out.println("Start of main method");
        // Producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put("batch.size", 16384);
        producerProperties.put("retries", 0);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("session.timeout.ms", 900000);
        // Construct Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);
    for(int i=0;i<10;i++) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, "hello from app"+i);
        //send
        try {
            System.out.println("Sending record...");
            kafkaProducer.send(producerRecord).get();
            System.out.println("Sent..");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        kafkaProducer.flush();
    }
        System.out.println("Closing ...!!!");
        kafkaProducer.close(10L, TimeUnit.MILLISECONDS);
    }
}
