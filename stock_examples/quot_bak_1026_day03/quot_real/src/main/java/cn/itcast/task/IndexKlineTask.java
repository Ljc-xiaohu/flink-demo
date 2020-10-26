package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.map.IndexKlineMapFunction;
import cn.itcast.function.sink.MySQLSink;
import cn.itcast.function.window.IndexMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 指数K线Task(日、周、月)
 */
public class IndexKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.数据处理
         * 4.编写插入sql
         * 5.（日、周、月）K线数据写入
         * 数据转换、分组
         * 数据写入mysql
         */
        //1.数据分组
        SingleOutputStreamOperator<IndexBean> applyData = waterData
                //分组
                .keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                //3.数据处理,获取指数分时行情数据
                .apply(new IndexMinutesWindowFunction());

        //4.编写插入sql
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

        //5.K线数据转换并写入MySQL
        //数据转换、分组
        //数据写入mysql
        //指数日K
        applyData.map(new IndexKlineMapFunction(KlineType.DAY_K.getType(), KlineType.DAY_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_INDEX_SQL_DAY_TABLE)))
                .setParallelism(2);

        //指数周K
        applyData.map(new IndexKlineMapFunction(KlineType.WEEK_K.getType(), KlineType.WEEK_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_INDEX_SQL_WEEK_TABLE)))
                .setParallelism(2);

        //指数月K
        applyData.map(new IndexKlineMapFunction(KlineType.MONTH_K.getType(), KlineType.MONTH_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_INDEX_SQL_MONTH_TABLE)))
                .setParallelism(2);
    }
}
