package Kafka;

import Bean.UserBehavior;
import Bean.UserBehaviorDecoder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.*;

public class Consumer implements Runnable{
    private static KafkaConsumer<String, UserBehavior> consumer;
    static{
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 1000);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", UserBehaviorDecoder.class.getName());
        consumer = new KafkaConsumer<String, UserBehavior>(props);
        consumer.subscribe(Arrays.asList("ub"));
    }
    public void run() {
        while (true){
            ConsumerRecords<String,UserBehavior> records = consumer.poll(1000);
             for (ConsumerRecord<String,UserBehavior> record:records) {
                 System.out.println(record.value().toString());
            }
        }
    }

}
