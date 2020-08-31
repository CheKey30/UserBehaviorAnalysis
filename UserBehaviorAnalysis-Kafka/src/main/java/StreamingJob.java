
import Kafka.Producer;


public class StreamingJob{
    public static void main(String[] args) {
        Producer producer = new Producer();
        Thread t1 = new Thread(producer);
        t1.start();
    }
}
