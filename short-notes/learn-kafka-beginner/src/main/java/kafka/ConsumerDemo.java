package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        // Consumer Properties
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,Utils.GROUP_ID);

        // Create consumer
        KafkaConsumer<String,Object> consumer = new KafkaConsumer<String, Object>(consumerProps);

        // Subscribe consumer
        consumer.subscribe(Arrays.asList(Utils.TOPIC_NAME));

        // Get data from consumer
        while(true){
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000L));

            for(ConsumerRecord<String,Object> record : records){

                log.info("Key : " + record.key() + " <--> Value : " + record.value());
                log.info("Topic : " + record.topic() + " <---> Partition : " + record.partition() + " <---> Offset : " + record.offset());
            }
        }
    }
}
