package com.learn.learnkafka.service;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ProducerService {

    private static Logger log = LoggerFactory.getLogger(ProducerService.class);

    public void produce(String topic,Object value) {

        Properties producerProp = new Properties();
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String,Object> producer = new KafkaProducer<>(producerProp);

        // Create record to produce
        ProducerRecord<String,Object> record = new ProducerRecord<>(topic,value.toString());
        // Send Data
        // It will not provide stats. So uncomment it to work with simple send without information
        // producer.send(record);

        // Now we will add a callback to get data out of it.
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {

                if(e == null){
                    log.info("Metadata : \n" +
                            "Topic : " + metadata.topic() + "\n" +
                            "Partition : " + metadata.partition() + "\n" +
                            "Offset : " + metadata.offset() + "\n" +
                            "Timestamp : " + metadata.timestamp());

                }else{
                    log.error("Error : " , e);
                }
            }
        });

        producer.flush();
        producer.close();
    }


    public void produce(String topic,String key, Object value) {

        Properties producerProp = new Properties();
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String,Object> producer = new KafkaProducer<>(producerProp);

        // Create record to produce
        ProducerRecord<String,Object> record = new ProducerRecord<>(topic,key,value.toString());
        // Send Data
        // It will not provide stats. So uncomment it to work with simple send without information
        // producer.send(record);

        // Now we will add a callback to get data out of it.
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {

                if(e == null){
                    log.info("Metadata : \n" +
                            "Topic : " + metadata.topic() + "\n" +
                            "Partition : " + metadata.partition() + "\n" +
                            "Offset : " + metadata.offset() + "\n" +
                            "Timestamp : " + metadata.timestamp());

                }else{
                    log.error("Error : " , e);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
