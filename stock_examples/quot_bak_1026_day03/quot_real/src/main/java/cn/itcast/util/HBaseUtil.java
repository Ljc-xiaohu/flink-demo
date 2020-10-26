package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBase工具类
 */
public abstract class HBaseUtil {
    /**
     * 开发步骤：
     * 0.定义成员变量Connection和Admin
     * 1.静态代码块获取连接对象
     * 2.获取表
     *  public static Table getTable(String tableNameStr,String columnFamily) {
     * 3.插入单列数据
     *  public static void putDataByRowkey(String tableName, String family, String colName, String colVal, String rowkey) {
     * 4.插入多列数据
     *  public static void putMapDataByRowkey(String tableName, String family, Map<String, Object> map, String rowkey) {
     * 5.根据rowkey查询数据
     *  public static String queryByRowkey(String tableName, String family, String colName, String rowkey) {
     * 6.根据rowkey删除数据
     *  public static void delByRowkey(String tableName, String family, String rowkey) {
     * 7.批量数据插入
     *  public static void putList(String tableName, List<Put> list) {
     * 8.测试
     */
    private static Connection conn;
    private static Admin admin;

    private static final Logger log = LoggerFactory.getLogger(HBaseUtil .class);

    //1.静态代码块获取连接对象
    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", QuotConfig.ZOOKEEPER_CONNECT);
        try {
            conn = ConnectionFactory.createConnection(config);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //2.获取表
    public static Table getTable(String tableNameStr,String columnFamily) {
        Table table = null;
        TableName tableName = TableName.valueOf(tableNameStr);
        // 构建表的描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        // 构建列族的描述器
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        hTableDescriptor.addFamily(hColumnDescriptor);
        // 如果表不存在则创建表
        try {
            if (!admin.tableExists(tableName)) {
                admin.createTable(hTableDescriptor);
            }
            table = conn.getTable(tableName);
        } catch (IOException e) {
            log.error("hbase连接异常", e);
        }
        return table;
    }

    //3.插入单列数据
    public static void putDataByRowkey(String tableName, String family, String colName, String colVal, String rowkey) {
        Table table = getTable(tableName,family);
        try {
            Put put = new Put(rowkey.getBytes());
            put.addColumn(family.getBytes(), colName.getBytes(), colVal.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //4.插入多列数据
    public static void putMapDataByRowkey(String tableName, String family, Map<String, Object> map, String rowkey) {
        Table table = getTable(tableName,family);
        try {
            Put put = new Put(rowkey.getBytes());
            for (String key : map.keySet()) {
                //map得key就是列名，value就是列值
                put.addColumn(family.getBytes(), key.getBytes(), map.get(key).toString().getBytes());
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //5.根据rowkey查询数据
    public static String queryByRowkey(String tableName, String family, String colName, String rowkey) {
        Table table = getTable(tableName,family);
        String str = null;
        try {
            Get get = new Get(rowkey.getBytes());
            Result result = table.get(get);
            byte[] value = result.getValue(family.getBytes(), colName.getBytes());
            if(value!=null){
                str = new String(value);
            }else{
                str = null;
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
        return str;
    }

    //6.根据rowkey删除数据
    public static void delByRowkey(String tableName, String family, String rowkey) {
        Table table = getTable(tableName,family);
        try {
            Delete delete = new Delete(rowkey.getBytes());
            delete.addFamily(family.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //7.批量数据插入
    public static void putList(String tableName, List<Put> list) {
        Table table = getTable(tableName,"info");
        try {
            table.put(list);
            System.out.println("数据已批量插入到HBase");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //8.测试
    public static void main(String[] args) {
        putDataByRowkey("test", "f1", "age", "20", "1");

        HashMap<String, Object> map = new HashMap<>();
        map.put("name", "jack");
        map.put("age", 21);
        putMapDataByRowkey("test", "f1", map, "2");

        String str = queryByRowkey("test", "f1", "age", "1");
        System.out.println(str);

        delByRowkey("test", "f1", "1");
        String str2 = queryByRowkey("test", "f1", "age", "1");
        System.out.println(str2);

        List<Put> list = new ArrayList<>();
        Put put1 = new Put("3".getBytes());
        put1.addColumn("f1".getBytes(), "age".getBytes(), "22".getBytes());
        Put put2 = new Put("4".getBytes());
        put2.addColumn("f1".getBytes(), "age".getBytes(), "23".getBytes());
        list.add(put1);
        list.add(put2);
        putList("test", list);
    }
}
