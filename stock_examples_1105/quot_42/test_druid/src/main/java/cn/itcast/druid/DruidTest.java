package cn.itcast.druid;

import java.sql.*;

/**
 * Author itcast
 * Date 2020/10/26 9:54
 * Desc 演示使用ApacheDruid数据库API
 * 注意:
 * Java代码操作数据之前学习过JDBC规范!而Druid也是遵循JDBC规范的
 * 所以直接导入Druid的驱动包,然后按照JDBC规范进行编码即可
 */
public class DruidTest {
    public static void main(String[] args) throws Exception {
        //1.加载驱动
        //Class.forName("org.apache.calcite.avatica.remote.Driver");
        //2.获取连接
        Connection conn = DriverManager.getConnection("jdbc:avatica:remote:url=http://192.168.52.100:8888/druid/v2/sql/avatica/");
        System.out.println("获取到的连接对象为:" + conn);

        //3.创建语句对象
        String sql = "SELECT user,views FROM \"metrics-kafka\"";
        Statement statement = conn.createStatement();
        //4.执行查询
        ResultSet rs = statement.executeQuery(sql);
        //5.结果集处理
        while (rs.next()){
            String user = rs.getString("user");
            long views = rs.getLong("views");
            System.out.println(user+" : "+views);
        }
        //6.关闭资源
        rs.close();
        statement.close();
        conn.close();
    }
}
