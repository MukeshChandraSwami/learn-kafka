package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Utils {

    public static final String TOPIC_NAME = "test-topic";
    public static final String GROUP_ID = "consumer-grp-1";

    public static Properties getProducerCommonProps(String bootstrapServers){

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProps;
    }

    public static KafkaProducer<String,String> getProducer(Properties props){

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }
}
