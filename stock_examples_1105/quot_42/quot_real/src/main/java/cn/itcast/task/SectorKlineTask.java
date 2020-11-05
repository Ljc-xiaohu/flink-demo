package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.map.SectorKlineMapFunction;
import cn.itcast.function.sink.MySQLSink;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author itcast
 * Date 2020/10/29 9:29
 * Desc 板块K线(日K,周K,月K)子业务核心处理类
 */
public class SectorKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //TODO 1.分组.keyBy(CleanBean::getSecCode)
        SingleOutputStreamOperator<SectorBean> sectorBeanDS = waterData.keyBy(CleanBean::getSecCode)
                //TODO 2.划分时间窗口.timeWindow(Time.minutes(1))
                .timeWindow(Time.minutes(1))
                //TODO 3.窗口数据处理为StockBean.apply(new StockMinutesWindowFunction());CleanBean-->StockBean
                .apply(new StockMinutesWindowFunction())
                //数据合并
                .timeWindowAll(Time.minutes(1))
                //StockBean转为SectorBean
                .apply(new SectorWindowFunction());
        //注意:
        //sectorBeanDS中已经包含了高开低收成交量成交金额,但没有均价,后续需要计算!
        //最后要把数据存入MySQL,所以先编写sql语句
        //TODO 4.编写插入sql="replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //注意:
        // replace表示替换/覆盖,如果没有值会插入,有值会替换/覆盖,这样可以保证每个个股每一天只有一条
        // 要实现这样的功能除了使用replace into还需要mysql中主键为日期+个股代码
        // 日K,周K,月K的表名不一样,所以需要使用占位符s%!
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //String sqlWithTableName = String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_DAY_TABLE);
        //TODO 5.K线数据转换,分:日K，周K，月K
        //日K
        //将StockBean转为Row行对象,并计算需要的均价...,Row对象方便后续进行?占位符参数设置!
        sectorBeanDS.map(new SectorKlineMapFunction(KlineType.DAY_K.getType(), KlineType.DAY_K.getFirstTxDateType()))
                //TODO 6.分组.keyBy(value -> value.getField(2))//SecCode
                .keyBy(row -> row.getField(2))
                //TODO 7.Sink到MySQL数据库.addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_DAY_TABLE)))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_DAY_TABLE)));

        //周K
        sectorBeanDS.map(new SectorKlineMapFunction(KlineType.WEEK_K.getType(), KlineType.WEEK_K.getFirstTxDateType()))
                //TODO 6.分组.keyBy(value -> value.getField(2))//SecCode
                .keyBy(row -> row.getField(2))
                //TODO 7.Sink到MySQL数据库.addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_DAY_TABLE)))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_WEEK_TABLE)));
        //月K
        sectorBeanDS.map(new SectorKlineMapFunction(KlineType.MONTH_K.getType(), KlineType.MONTH_K.getFirstTxDateType()))
                //TODO 6.分组.keyBy(value -> value.getField(2))//SecCode
                .keyBy(row -> row.getField(2))
                //TODO 7.Sink到MySQL数据库.addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_DAY_TABLE)))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_SECTOR_SQL_MONTH_TABLE)));

    }
}
