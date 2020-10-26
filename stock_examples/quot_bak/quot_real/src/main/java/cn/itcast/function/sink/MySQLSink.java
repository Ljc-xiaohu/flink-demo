package cn.itcast.function.sink;

import cn.itcast.util.DBUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * K线数据保存到MySQL
 */
public class MySQLSink extends RichSinkFunction<Row> {
    /**
     * 开发步骤：
     * 1.创建构造方法，入参：插入sql
     * 2.初始化mysql
     * 获取JDBC连接，开启事务
     * 3.封装预处理对象
     * 执行更新操作
     * 提交事务
     * 4.关流
     */
    private String sql;

    public MySQLSink(String sql) {
        this.sql = sql;
    }

    Connection conn = null;
    PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //2.初始化mysql
        //获取JDBC连接，开启事务
        conn = DBUtil.getConnByJdbc();
        //conn.setAutoCommit(false);//开启事务
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        //执行更新操作
        int arity = value.getArity();//length
        for (int i = 0; i < arity; i++) {
            ps.setObject(i + 1, value.getField(i));//jdbc序号从1开始
        }
        ps.executeUpdate();
        System.out.println("数据已插入到MySQL");
        //conn.commit();
    }


    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
