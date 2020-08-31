package Bean;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class UserBehaviorDecoder implements Deserializer<UserBehavior> {

    public void configure(Map<String, ?> map, boolean b) {

    }

    public UserBehavior deserialize(String s, byte[] bytes) {
        return (UserBehavior) BeanUtils.BytesToObject(bytes);
    }

    public void close() {

    }
}
