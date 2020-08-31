package FlinkHandlers;

import Bean.BeanUtils;
import Bean.UserBehavior;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaMsgSchema implements DeserializationSchema<UserBehavior>, SerializationSchema<UserBehavior> {

    @Override
    public UserBehavior deserialize(byte[] bytes) throws IOException {
        return (UserBehavior) BeanUtils.BytesToObject(bytes);
    }

    @Override
    public boolean isEndOfStream(UserBehavior userBehavior) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehavior userBehavior) {
        return BeanUtils.ObjectToBytes(userBehavior);
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
