import Bean.ItemViewCount;
import Bean.UserBehavior;
import ElasticSearch.RestClientFactoryImpl;
import FlinkHandlers.CountAgg;
import FlinkHandlers.KafkaMsgSchema;
import FlinkHandlers.TopHotItems;
import FlinkHandlers.WindowResultFunction;
import MySQL.MySQLSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.*;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableForceAvro();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.enableCheckpointing(5000L);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<UserBehavior> stream = env.addSource(new FlinkKafkaConsumer<UserBehavior>("ub", new KafkaMsgSchema(), properties)).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimeStamp();
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        });
        // put data into es
        HashMap<String,String> config = new HashMap<String,String>();
        config.put("cluster.name", "elasticsearch");
        config.put("bulk.flush.max.actions", "1");
        List<HttpHost> transportAddresses = new ArrayList<>();
        transportAddresses.add(new HttpHost("localhost",9200,"http"));
        ElasticsearchSink.Builder<UserBehavior> esSinkBuilder = new ElasticsearchSink.Builder<>(transportAddresses,
                new ElasticsearchSinkFunction<UserBehavior>() {
                    public IndexRequest createIndexRequest(UserBehavior userBehavior){
                        Map<String,String> json = new HashMap<>();
                        json.put("itemId",String.valueOf(userBehavior.getItemId()));
                        json.put("categoryId",String.valueOf(userBehavior.getCategoryId()));
                        json.put("userId",String.valueOf(userBehavior.getUserId()));
                        json.put("behavior",userBehavior.getBehavior());
                        json.put("timeStamp",String.valueOf(userBehavior.getTimeStamp()));
                        return Requests.indexRequest().index("ub").type("FE").source(json);
                    }

                    @Override
                    public void process(UserBehavior userBehavior, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(userBehavior));
                    }
                });

        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        stream.addSink(esSinkBuilder.build());
        DataStream<ItemViewCount> viewData = stream.keyBy("itemId").timeWindow(Time.minutes(5),Time.minutes(1)).aggregate(new CountAgg(),new WindowResultFunction());
        viewData.addSink(new MySQLSink());
        DataStream<String> topN = viewData.keyBy("windowEnd").process(new TopHotItems(5));
        topN.print();
        env.execute("test");

    }

}
