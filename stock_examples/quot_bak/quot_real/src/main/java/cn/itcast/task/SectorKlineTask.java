package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.map.SectorKlineMapFunction;
import cn.itcast.function.sink.MySQLSink;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 板块K线(日、周、月)
 */
public class SectorKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.数据分组
         * 2.划分时间窗口
         * 3.数据处理
         * 4.编写插入sql
         * 5.（日、周、月）K线数据写入
         * 数据转换、分组
         * 数据写入mysql
         */
        SingleOutputStreamOperator<SectorBean> sectorData = waterData
                .keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))//板块数据来自于个股，首先获取个股数据
                .apply(new StockMinutesWindowFunction())
                .timeWindowAll(Time.minutes(1))
                .apply(new SectorWindowFunction());

        //4.编写插入sql
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //5.K线数据转换并写入MySQL
        //日K
        sectorData.map(new SectorKlineMapFunction(KlineType.DAY_K.getType(),KlineType.DAY_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_DAY_TABLE)))
                .setParallelism(2);
        //周K
        sectorData.map(new SectorKlineMapFunction(KlineType.WEEK_K.getType(),KlineType.WEEK_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_WEEK_TABLE)))
                .setParallelism(2);

        //月K
        sectorData.map(new SectorKlineMapFunction(KlineType.MONTH_K.getType(),KlineType.MONTH_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_MONTH_TABLE)))
                .setParallelism(2);
    }
}
