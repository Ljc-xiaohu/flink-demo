package cn.itcast;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 测试Kylin-JDBC
 */
public class KylinTest {
    public static void main(String[] args) throws Exception {
        //1.加载驱动
        //Class.forName("org.apache.kylin.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:kylin://node01:7070/itcast_quot_dm", "ADMIN", "KYLIN");
        // 3.获取查询对象
        Statement st = conn.createStatement();
        //4.执行查询sql
        String sql = "SELECT\n" +
                "  sec_code,\n" +
                "  trade_date,\n" +
                "  MAX(high_price) as high_price,\n" +
                "  MIN(low_price) as low_price,\n" +
                "  MAX(trade_vol_day) as  trade_vol_max,\n" +
                "  MIN(trade_vol_day) as  trade_vol_min,\n" +
                "  MAX(trade_amt_day) as  trade_amt_max,\n" +
                "  MIN(trade_amt_day) as  trade_amt_min\n" +
                "FROM itcast_quot_dm.app_sec_quot_stock\n" +
                "GROUP BY 1,2\n" +
                "ORDER BY sec_code\n" +
                "LIMIT 10;";
        ResultSet rs = st.executeQuery(sql);
        //5.获取结果
        while (rs.next()) {
            String code = rs.getString(1);
            String date = rs.getString(2);
            String highPrice = rs.getString("high_price");
            System.out.println(date + "," + code + "," + highPrice);
        }
        // 6.关流
        rs.close();
        st.close();
        conn.close();
    }
}
