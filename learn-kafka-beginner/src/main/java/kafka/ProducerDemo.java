package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {

        // Producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer.
        KafkaProducer<String,Object> producer = new KafkaProducer<String, Object>(producerProps);

        try {
            for (int i = 0; i < 10; i++) {

                String key = "id_" + i;
                String value = "msg - " + i;
                // Create a record
                ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(Utils.TOPIC_NAME, key, value);

                log.info("Key : " + key);

                //Send Data
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception exce) {

                        if(exce == null) {
                            String metadata = "Metdata : \n"
                                    + "Topic : " + recordMetadata.topic() + "\n"
                                    + "Partition : " + recordMetadata.partition() + "\n"
                                    + "Offset : " + recordMetadata.offset();
                            log.info(metadata);
                        }else{
                            log.error("Error : " + exce);
                        }
                    }
                }).get();
            }
        }catch(Exception e){
            log.error("Error : " + e);
        }

    }
}
