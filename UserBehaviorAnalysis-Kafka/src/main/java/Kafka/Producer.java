package Kafka;

import Bean.UserBehavior;
import Bean.UserBehaviorEncoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.*;
import java.net.URISyntaxException;
import java.util.Properties;

public class Producer implements Runnable {
    private static Properties props;
    private static KafkaProducer<String, UserBehavior> producer;
    private static BufferedReader bf;
    private static String currentLine;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", UserBehaviorEncoder.class.getName());
        producer = new KafkaProducer<String, UserBehavior>(props);
        try {
            bf = new BufferedReader(new FileReader(new File(Producer.class.getClassLoader().getResource("UserBehavior.csv").toURI())));
            currentLine = bf.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        //每一秒向kafka发送一条消息
        while (currentLine != null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String[] parts = currentLine.split(",");
            UserBehavior ub = new UserBehavior();
            ub.setUserId(Long.valueOf(parts[0]));
            ub.setItemId(Long.valueOf(parts[2]));
            ub.setCategoryId(Long.valueOf(parts[1]));
            ub.setBehavior(parts[3]);
            ub.setTimeStamp(System.currentTimeMillis());
            producer.send(new ProducerRecord<String, UserBehavior>("ub", "behavior",ub));
            try {
                currentLine = bf.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
