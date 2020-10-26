package cn.itcast.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @Date 2020/9/24
 * 建立Apache Druid JDBC连接
 */
public abstract class DruidUtil {
    private static DataSource dataSource = null;
    static {
        //加载配置文件
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        try {
            Properties props = new Properties();
            props.load(in);
            Properties druidProps = new Properties();
            druidProps.setProperty("driverClassName",props.getProperty("druid.driverClassName"));
            druidProps.setProperty("url",props.getProperty("druid.url"));
            dataSource = DruidDataSourceFactory.createDataSource(druidProps);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //1.获取连接
    public static Connection getConn(){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    //2.关闭资源
    public static void close(ResultSet rs, Statement st,Connection conn){
        try {
            rs.close();
            st.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        Connection conn = getConn();
        System.out.println(conn);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT * FROM \"index_stream_sse\" limit 5");
        while (rs.next()) {
            String indexCode = rs.getString("indexCode");
            String indexName = rs.getString("indexName");
            System.out.println(indexCode + ":" + indexName);
        }
        close(rs,st,conn);
    }
}
