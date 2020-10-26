package cn.itcast;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 使用JDBC操作Druid，获取实时分析结果
 */
public class DruidTest {
    public static void main(String[] args) throws Exception {
        //1.加载驱动
        //Class.forName("org.apache.calcite.avatica.remote.Driver");
        //2.建立连接
        Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://node01:8888/druid/v2/sql/avatica/");
        //3.获取statement
        Statement st = connection.createStatement();
        //4.执行sql查询
        //String sql = "SELECT * FROM \"metrics-kafka\"";
        String sql = "SELECT user, sum(views) as view_count FROM \"metrics-kafka\" GROUP BY 1 ORDER BY 1";
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()){
            //5.获取查询结果
            System.out.println(rs.getString(1)+":"+rs.getString(2));
        }
        //6.关流
        rs.close();
        st.close();
        connection.close();
    }
}
