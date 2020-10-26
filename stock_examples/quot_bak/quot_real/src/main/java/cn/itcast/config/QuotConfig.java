package cn.itcast.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 加载配置文件config.properties
 */
public class QuotConfig {
    public static String DATE = null;
    public static String CLOSE_TIME = null;
    public static String OPEN_TIME = null;
    //#kafka配置
    public static String BOOTSTRAP_SERVERS = null;
    public static String ZOOKEEPER_CONNECT = null;
    public static String SSE_TOPIC = null;
    public static String SZSE_TOPIC = null;
    public static String GROUP_ID = null;
    //#MySQL
    public static String MYSQL_DRIVER = null;
    public static String MYSQL_URL = null;
    public static String MYSQL_USERNAME = null;
    public static String MYSQL_PASSWORD = null;
    //#Redis
    public static String REDIS_HOST = null;
    public static String REDIS_MAXTOTAL = null;
    public static String REDIS_MINIDLE = null;
    public static String REDIS_MAXIDLE = null;
    //#水位线延时时间
    public static String DELAY_TIME = null;
    //#个股分时topic
    public static String SSE_STOCK_TOPIC = null;
    public static String SZSE_STOCK_TOPIC = null;
    //#个股分时涨跌幅
    public static String STOCK_INCREASE_TOPIC = null;
    //#指数分时topic
    public static String SSE_INDEX_TOPIC = null;
    public static String SZSE_INDEX_TOPIC = null;
    //#板块分时topic
    public static String SSE_SECTOR_TOPIC = null;
    public static String SZSE_SECTOR_TOPIC = null;
    public static String SECTOR_TOPIC = null;
    //#分时数据写入HDFS参数
    public static String HDFS_BUCKETER = null;
    //#1G
    public static String HDFS_BATCH = null;
    //#字段分隔符
    public static String HDFS_SEPERATOR = null;
    //#个股分时HDFS路径
    public static String STOCK_SEC_HDFS_PATH = null;
    //#指数分时HDFS路径
    public static String INDEX_SEC_HDFS_PATH = null;
    //#板块分时HDFS路径
    public static String SECTOR_SEC_HDFS_PATH = null;
    //#个股Hbase表名
    public static String STOCK_HBASE_TABLE_NAME = null;
    //#指数Hbase表名
    public static String INDEX_HBASE_TABLE_NAME = null;
    //#板块Hbase表名
    public static String SECTOR_HBASE_TABLE_NAME = null;
    //#k线写入mysql表名
    //#个股日k
    public static String MYSQL_STOCK_SQL_DAY_TABLE = null;
    //#个股周k
    public static String MYSQL_STOCK_SQL_WEEK_TABLE = null;
    //#个股月k
    public static String MYSQL_STOCK_SQL_MONTH_TABLE = null;
    //#指数日k
    public static String MYSQL_INDEX_SQL_DAY_TABLE = null;
    //#指数周k
    public static String MYSQL_INDEX_SQL_WEEK_TABLE = null;
    //#指数月k
    public static String MYSQL_INDEX_SQL_MONTH_TABLE = null;
    //#板块日k
    public static String MYSQL_SECTOR_SQL_DAY_TABLE = null;
    //#板块周k
    public static String MYSQL_SECTOR_SQL_WEEK_TABLE = null;
    //#板块月k
    public static String MYSQL_SECTOR_SQL_MONTH_TABLE = null;


    public static Properties config = new Properties();
    static {
        //加载配置文件
        //InputStream in = QuotConfig.class.getClassLoader().getResourceAsStream("config.properties");
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
        try {
            config.load(in);
            DATE = config.getProperty("date");
            CLOSE_TIME = config.getProperty("close.time");
            OPEN_TIME = config.getProperty("open.time");
            BOOTSTRAP_SERVERS = config.getProperty("bootstrap.servers");
            ZOOKEEPER_CONNECT = config.getProperty("zookeeper.connect");
            SSE_TOPIC = config.getProperty("sse.topic");
            SZSE_TOPIC = config.getProperty("szse.topic");
            GROUP_ID = config.getProperty("group.id");
            MYSQL_DRIVER = config.getProperty("mysql.driver");
            MYSQL_URL = config.getProperty("mysql.url");
            MYSQL_USERNAME = config.getProperty("mysql.username");
            MYSQL_PASSWORD = config.getProperty("mysql.password");
            REDIS_HOST = config.getProperty("redis.host");
            REDIS_MAXTOTAL = config.getProperty("redis.maxTotal");
            REDIS_MINIDLE = config.getProperty("redis.minIdle");
            REDIS_MAXIDLE = config.getProperty("redis.maxIdle");
            DELAY_TIME = config.getProperty("delay.time");
            SSE_STOCK_TOPIC = config.getProperty("sse.stock.topic");
            SZSE_STOCK_TOPIC = config.getProperty("szse.stock.topic");
            STOCK_INCREASE_TOPIC = config.getProperty("stock.increase.topic");
            SSE_INDEX_TOPIC = config.getProperty("sse.index.topic");
            SZSE_INDEX_TOPIC = config.getProperty("szse.index.topic");
            SSE_SECTOR_TOPIC = config.getProperty("sse.sector.topic");
            SZSE_SECTOR_TOPIC = config.getProperty("szse.sector.topic");
            SECTOR_TOPIC = config.getProperty("sector.topic");
            HDFS_BUCKETER = config.getProperty("hdfs.bucketer");
            HDFS_BATCH = config.getProperty("hdfs.batch");
            HDFS_SEPERATOR = config.getProperty("hdfs.seperator");
            STOCK_SEC_HDFS_PATH = config.getProperty("stock.sec.hdfs.path");
            INDEX_SEC_HDFS_PATH = config.getProperty("index.sec.hdfs.path");
            SECTOR_SEC_HDFS_PATH = config.getProperty("sector.sec.hdfs.path");
            STOCK_HBASE_TABLE_NAME = config.getProperty("stock.hbase.table.name");
            INDEX_HBASE_TABLE_NAME = config.getProperty("index.hbase.table.name");
            SECTOR_HBASE_TABLE_NAME = config.getProperty("sector.hbase.table.name");
            MYSQL_STOCK_SQL_DAY_TABLE = config.getProperty("mysql.stock.sql.day.table");
            MYSQL_STOCK_SQL_WEEK_TABLE = config.getProperty("mysql.stock.sql.week.table");
            MYSQL_STOCK_SQL_MONTH_TABLE = config.getProperty("mysql.stock.sql.month.table");
            MYSQL_INDEX_SQL_DAY_TABLE = config.getProperty("mysql.index.sql.day.table");
            MYSQL_INDEX_SQL_WEEK_TABLE = config.getProperty("mysql.index.sql.week.table");
            MYSQL_INDEX_SQL_MONTH_TABLE = config.getProperty("mysql.index.sql.month.table");
            MYSQL_SECTOR_SQL_DAY_TABLE = config.getProperty("mysql.sector.sql.day.table");
            MYSQL_SECTOR_SQL_WEEK_TABLE = config.getProperty("mysql.sector.sql.week.table");
            MYSQL_SECTOR_SQL_MONTH_TABLE = config.getProperty("mysql.sector.sql.month.table");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("close.time = "+config.getProperty("close.time"));
        System.out.println("DATE = "+DATE);
        System.out.println("CLOSE_TIME = "+CLOSE_TIME);
        System.out.println("OPEN_TIME = "+OPEN_TIME);
        System.out.println("BOOTSTRAP_SERVERS = "+BOOTSTRAP_SERVERS);
        System.out.println("ZOOKEEPER_CONNECT = "+ZOOKEEPER_CONNECT);
        System.out.println("SSE_TOPIC = "+SSE_TOPIC);
        System.out.println("SZSE_TOPIC = "+SZSE_TOPIC);
        System.out.println("GROUP_ID = "+GROUP_ID);
        System.out.println("MYSQL_DRIVER = "+MYSQL_DRIVER);
        System.out.println("MYSQL_URL = "+MYSQL_URL);
        System.out.println("MYSQL_USERNAME = "+MYSQL_USERNAME);
        System.out.println("MYSQL_PASSWORD = "+MYSQL_PASSWORD);
        System.out.println("REDIS_HOST = "+REDIS_HOST);
        System.out.println("REDIS_MAXTOTAL = "+REDIS_MAXTOTAL);
        System.out.println("REDIS_MINIDLE = "+REDIS_MINIDLE);
        System.out.println("REDIS_MAXIDLE = "+REDIS_MAXIDLE);
        System.out.println("DELAY_TIME = "+DELAY_TIME);
        System.out.println("SSE_STOCK_TOPIC = "+SSE_STOCK_TOPIC);
        System.out.println("SZSE_STOCK_TOPIC = "+SZSE_STOCK_TOPIC);
        System.out.println("STOCK_INCREASE_TOPIC = "+STOCK_INCREASE_TOPIC);
        System.out.println("SSE_INDEX_TOPIC = "+SSE_INDEX_TOPIC);
        System.out.println("SZSE_INDEX_TOPIC = "+SZSE_INDEX_TOPIC);
        System.out.println("SSE_SECTOR_TOPIC = "+SSE_SECTOR_TOPIC);
        System.out.println("SZSE_SECTOR_TOPIC = "+SZSE_SECTOR_TOPIC);
        System.out.println("SECTOR_TOPIC = "+SECTOR_TOPIC);
        System.out.println("HDFS_BUCKETER = "+HDFS_BUCKETER);
        System.out.println("HDFS_BATCH = "+HDFS_BATCH);
        System.out.println("HDFS_SEPERATOR = "+HDFS_SEPERATOR);
        System.out.println("STOCK_SEC_HDFS_PATH = "+STOCK_SEC_HDFS_PATH);
        System.out.println("INDEX_SEC_HDFS_PATH = "+INDEX_SEC_HDFS_PATH);
        System.out.println("SECTOR_SEC_HDFS_PATH = "+SECTOR_SEC_HDFS_PATH);
        System.out.println("STOCK_HBASE_TABLE_NAME = "+STOCK_HBASE_TABLE_NAME);
        System.out.println("INDEX_HBASE_TABLE_NAME = "+INDEX_HBASE_TABLE_NAME);
        System.out.println("SECTOR_HBASE_TABLE_NAME = "+SECTOR_HBASE_TABLE_NAME);
        System.out.println("MYSQL_STOCK_SQL_DAY_TABLE = "+MYSQL_STOCK_SQL_DAY_TABLE);
        System.out.println("MYSQL_STOCK_SQL_WEEK_TABLE = "+MYSQL_STOCK_SQL_WEEK_TABLE);
        System.out.println("MYSQL_STOCK_SQL_MONTH_TABLE = "+MYSQL_STOCK_SQL_MONTH_TABLE);
        System.out.println("MYSQL_INDEX_SQL_DAY_TABLE = "+MYSQL_INDEX_SQL_DAY_TABLE);
        System.out.println("MYSQL_INDEX_SQL_WEEK_TABLE = "+MYSQL_INDEX_SQL_WEEK_TABLE);
        System.out.println("MYSQL_INDEX_SQL_MONTH_TABLE = "+MYSQL_INDEX_SQL_MONTH_TABLE);
        System.out.println("MYSQL_SECTOR_SQL_DAY_TABLE = "+MYSQL_SECTOR_SQL_DAY_TABLE);
        System.out.println("MYSQL_SECTOR_SQL_WEEK_TABLE = "+MYSQL_SECTOR_SQL_WEEK_TABLE);
        System.out.println("MYSQL_SECTOR_SQL_MONTH_TABLE = "+MYSQL_SECTOR_SQL_MONTH_TABLE);
    }
}
