package org.example;

import com.sun.org.apache.xalan.internal.xsltc.compiler.Pattern;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Map<Long,Long> offsetInPartition=new HashMap<>();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("test",0));
        consumer.assign(topicPartitions);
        while(true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,String> p:poll){
                System.out.println(p.value());
            }
            consumer.commitAsync(new OffsetCommitCallback() {

                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if(e!=null){
                        System.out.println("We can do it");
                    }else{
                          for(TopicPartition key:map.keySet()){
                              offsetInPartition.put((long)key.partition(),map.get(key).offset());
                              System.out.println("The partition is "+key.partition());
                              System.out.println("The offset is "+map.get(key).offset());
                          }
                    }
                }
            });
        }
    }
}
