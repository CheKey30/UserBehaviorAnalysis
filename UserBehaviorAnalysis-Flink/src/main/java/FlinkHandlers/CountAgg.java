package FlinkHandlers;

import Bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0l;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
        return acc+1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc, Long acc1) {
        return acc1+acc;
    }
}
