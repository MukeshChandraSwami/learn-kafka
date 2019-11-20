package kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("Following Partition has been revoked...");

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {


    }
}
