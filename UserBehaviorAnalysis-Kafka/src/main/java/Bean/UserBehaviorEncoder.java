package Bean;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserBehaviorEncoder implements  Serializer<UserBehavior> {

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, UserBehavior userBehavior) {
        return BeanUtils.ObjectToBytes(userBehavior);
    }

    public void close() {

    }
}
