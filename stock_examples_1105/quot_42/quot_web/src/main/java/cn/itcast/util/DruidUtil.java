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
 * Author itcast
 * Date 2020/11/4 10:23
 * Desc apache druid工具类
 * 通过该工具类可以获取apache druid的连接,要求整合alibaba druid连接池
 * 当然也可以整合其他连接池,如dbcp,c3p0
 * 注意:
 * 工具类应该让人家直接使用类名.方法名就可以调用的,而不需要new,也没必要new
 * 所以可以将构造私有,或将类使用abstract修饰
 */
public abstract class DruidUtil {
    //定义连接池-初始化一次就够了
    private static DataSource dataSource = null;

    //静态代码块-类加载时执行一次
    static{
        //-1.加载配置
        //加载配置文件,根据配置初始化连接池
        //使用当前线程的累加值器从类路径/编译路径下加载指定名称的配置文件,并返回InputStream
        //那么现在配置的kv就在InputStream中
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);//从InputStream中加载kv到properties中,那么现在kv在properties中
            //-2.获取需要的配置
            String url = properties.getProperty("druid.url");
            String driverClassName = properties.getProperty("druid.driverClassName");
            Properties druidProperties = new Properties();//单独创建一个Properties里面封装连接apache druid需要的参数
            druidProperties.setProperty("url", url);
            druidProperties.setProperty("driverClassName", driverClassName);
            //-3.根据配置创建alibaba druid连接池
            dataSource = DruidDataSourceFactory.createDataSource(druidProperties);

        } catch (Exception e) {
            throw new RuntimeException("连接池初始化异常! 请检测url,用户名,密码等相关配置和服务器状态");
        }
    }

    public static Connection getConn(){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (Exception e) {
            //注意:
            // 工具类是为了方便别人使用的,所以遇到异常时,尽量try catch住处理完
            // 如果该异常发生了处理不了,可以抛出运行时异常
            // 运行时异常的特点:使用者在编译时不需要处理
            throw new RuntimeException("apache druid connection初始化异常! 请检测url,用户名,密码等相关配置和服务器状态");
        }
        return conn;
    }
    public static void close(ResultSet rs, Statement st, Connection conn) {
        try {
            if (rs!=null){
                rs.close();//真正的关闭
            }
            if(st!= null){
                st.close();//真正的关闭
            }
            if(conn!= null){
                conn.close();//表面上是关闭,实际上是返回给连接池!
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection conn = DruidUtil.getConn();//人家使用的时候可以直接类名.方法名
        System.out.println(conn);
    }

}
