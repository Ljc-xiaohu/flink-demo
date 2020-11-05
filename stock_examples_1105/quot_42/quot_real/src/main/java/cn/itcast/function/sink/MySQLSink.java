package cn.itcast.function.sink;

import cn.itcast.util.DBUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Author itcast
 * Date 2020/10/29 11:06
 * Desc
 */
public class MySQLSink extends RichSinkFunction<Row> {
    private String sqlWithTableName;
    public MySQLSink(String sqlWithTableName) {
        System.out.println("sql为:" + sqlWithTableName);
        this.sqlWithTableName = sqlWithTableName;
    }
    private Connection conn;
    private PreparedStatement ps;
    //1开启连接
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DBUtil.getConnByJdbc();
        //"replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        ps = conn.prepareStatement(sqlWithTableName);
    }
    //3.设置参数并执行
    @Override
    public void invoke(Row row, Context context) throws Exception {
        int length = row.getArity();//13
        for (int i = 0; i < length; i++) {//注意row底层是数组,index从0开始
            Object value = row.getField(i);
            ps.setObject(i + 1, value);//JDBC的?从1开始
        }
        Object tpye = row.getField(4);
        System.out.println("type为:"+tpye);
        ps.executeUpdate();//执行
    }

    //3.关闭连接
    @Override
    public void close() throws Exception {
        if (conn != null) conn.close();
        if (ps != null) ps.close();
    }
}
