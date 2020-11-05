package cn.itcast.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Date 2020/9/24
 * HBase查询工具类
 */
public class HBaseUtil {
    public static Connection conn;
    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181");
        try {
            conn = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 指数行情区间查询
     */
    public static List<String> queryScan(String tableName, String family, String colName, String startKey, String endKey) {
        TableName hbaseTableName = TableName.valueOf(tableName);
        List<String> list = null;
        Table table = null;
        try {
            table = conn.getTable(hbaseTableName);
            Scan scan = new Scan();
            scan.setStartRow(startKey.getBytes());
            scan.setStopRow(endKey.getBytes());
            //查询
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<>();
            for (Result result : scanner) {
                byte[] value = result.getValue(family.getBytes(), colName.getBytes());
                list.add(new String(value));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static void main(String[] args) {
        List<String> list = queryScan("quot_index", "info", "data", "00000220201012010824", "00000220201012010864");
        list.forEach(System.out::println);
    }
}
