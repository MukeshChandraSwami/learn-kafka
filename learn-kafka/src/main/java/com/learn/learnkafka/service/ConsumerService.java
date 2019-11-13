package com.learn.learnkafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Service
public class ConsumerService {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public String consume(String group, String topic) {

        // Setting Properties
        Properties consumerProp = new Properties();
        consumerProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        consumerProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        consumerProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create consumer
        KafkaConsumer<String,Object> consumer = new KafkaConsumer<String, Object>(consumerProp);

        // Subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        // Get the data
        while(true) {
            ConsumerRecords<String,Object> records = consumer.poll(Duration.ofMillis(1000));
            break;
        }
        return null;
    }
}
