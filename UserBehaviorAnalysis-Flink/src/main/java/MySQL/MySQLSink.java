package MySQL;

import Bean.ItemViewCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink extends RichSinkFunction<ItemViewCount> {
    private PreparedStatement ps = null;
    private Connection connection = null;
    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://localhost:3306/UserBehavior?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    String username  = "root";
    String password = "123456";

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url,username,password);
        String sql = "insert into ItemViewCount (itemId,windowEnd,viewCount) value (?,?,?);";
        ps = connection.prepareStatement(sql);

    }

    @Override
    public void invoke(ItemViewCount value, Context context) throws Exception{
        ps.setLong(1,value.itemId);
        ps.setLong(2,value.windowEnd);
        ps.setLong(3,value.viewCount);
//        System.out.println("insert success");
        ps.execute();
    }

    @Override
    public void close() throws Exception{
        super.close();
        if(connection!=null){
            connection.close();
        }
        if(ps!=null){
            ps.close();
        }
    }

}
