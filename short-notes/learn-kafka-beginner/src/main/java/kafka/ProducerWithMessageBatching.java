package kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithMessageBatching {

    private static Logger log = LoggerFactory.getLogger(ProducerWithMessageBatching.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static void main(String[] args) {
       Properties producerProps = Utils.getProducerCommonProps(BOOTSTRAP_SERVER);

       // Creating more throughput via setting batch.size and linger.ms
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG,Integer.toString(20));
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32 * 1024));
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");


       KafkaProducer<String,String> kafkaProducer = Utils.getProducer(producerProps);

       int i = 0;

       while(true) {

           i = (i == 100) ? 0 : i;

           String key = "id_" + i;
           String value = "data_" + i;

           log.info("Key : " + key + " Value : " + value);

           ProducerRecord<String, String> producerRecord = new ProducerRecord<>(Utils.TOPIC_NAME, key, value);
           kafkaProducer.send(producerRecord, new Callback() {
               @Override
               public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        log.error("ERROR : ", e);
                        System.exit(1);
                    } else {
                        String metadata = "Metdata : \n"
                                + "Topic : " + recordMetadata.topic() + "\n"
                                + "Partition : " + recordMetadata.partition() + "\n"
                                + "Offset : " + recordMetadata.offset();
                        log.info(metadata);
                    }
               }
           });
       }
    }
}
