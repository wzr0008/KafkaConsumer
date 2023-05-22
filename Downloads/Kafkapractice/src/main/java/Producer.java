import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.CustomerInterceptor;
import org.example.CustomerPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        CustomerPartition customerPartition = new CustomerPartition();
        List<String> interceptors=new ArrayList<>();
        interceptors.add(CustomerInterceptor.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomerPartition.class.getName());
        properties.put(ProducerConfig.LINGER_MS_CONFIG,100);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"lz4");
        KafkaProducer<String, String> prod = new KafkaProducer<String, String>(properties);
        for(int i=0;i<=100;i++){
            if(i<=50) {
                prod.send(new ProducerRecord<String, String>("test","数据已经压缩"), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("数据已经发往 " + recordMetadata.topic() + " 位置是 " + recordMetadata.partition());
                        }
                    }
                });
            } else{
                prod.send(new ProducerRecord<String, String>("test", "曾鹏早晚要死绝"), new Callback() {

                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                System.out.println("数据已经发往 " + recordMetadata.topic() + " 位置是 " + recordMetadata.partition());
                            }
                        }
                    });
            }
        }
        prod.send(new ProducerRecord<String,String>("test","Hello from Kafka"));
        prod.close();
    }
}
