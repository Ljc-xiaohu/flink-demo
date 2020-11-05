package cn.itcast.kylin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Author itcast
 * Date 2020/11/2 9:07
 * Desc
 */
public class TestKylin {
    public static void main(String[] args) throws Exception {
        //0.加载驱动--可以省略
        //Class.forName("org.apache.kylin.jdbc.Driver");
        //1.获取连接
        Connection conn = DriverManager.getConnection("jdbc:kylin://192.168.52.100:7070/itcast_quot_dm2", "ADMIN", "KYLIN");
        //2.获取Statement对象
        Statement st = conn.createStatement();
        //3.编写sql
        String sql = "select a.trade_date,a.sec_code,a.sec_name,\n" +
                "round(max(a.trade_vol_day) / b.nego_cap,2) hsl\n" +
                "from app_sec_quot_stock a\n" +
                "join app_sec_sector_stock3 b on a.sec_code = b.sec_code\n" +
                "group by a.trade_date,a.sec_code,a.sec_name,b.nego_cap\n" +
                "order by hsl desc;";
        //4.执行sql
        ResultSet rs = st.executeQuery(sql);
        //5.获取结果集
        while (rs.next()) {
            Object sec_code = rs.getObject("sec_code");
            Object hsl = rs.getObject("hsl");
            System.out.println("股票代码: "+ sec_code + " 换手率:" + hsl);
        }
        //6.关闭资源
        rs.close();
        st.close();
        conn.close();
    }
}
