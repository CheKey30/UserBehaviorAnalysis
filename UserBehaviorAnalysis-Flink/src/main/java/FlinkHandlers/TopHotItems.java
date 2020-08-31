package FlinkHandlers;

import Bean.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class TopHotItems extends KeyedProcessFunction<Tuple, ItemViewCount,String> {
    private final int topSize;
    public TopHotItems(int n){
        this.topSize = n;
    }

    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<ItemViewCount>("ItemState-state",ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(ItemViewCount input,Context context, Collector<String> collector)throws Exception{
        itemState.add(input);
        context.timerService().registerEventTimeTimer(input.windowEnd+1);
    }

    @Override
    public void onTimer(long timestamp,OnTimerContext ctx, Collector<String> out)throws Exception{
        List<ItemViewCount>  allItems = new ArrayList<>();
        for(ItemViewCount item:itemState.get()){
            allItems.add(item);
        }
        itemState.clear();
        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.viewCount-o1.viewCount);
            }
        });

        StringBuffer res = new StringBuffer();
        res.append("==================");
        res.append("Time: ").append(new Timestamp(timestamp-1)).append("\n");
        for(int i=0;i<topSize;i++){
            ItemViewCount curr = allItems.get(i);
            res.append("No ").append(i).append(":")
                    .append("itemId: ").append(curr.itemId)
                    .append(" ViewCount: ").append(curr.viewCount)
                    .append("\n");
        }
        res.append("==================\n\n");
        out.collect(res.toString());
    }
}
