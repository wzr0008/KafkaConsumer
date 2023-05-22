package org.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomerPartition implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String value = o1.toString();
        if(value.contains("Hello")){
            return 1;
        }
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
