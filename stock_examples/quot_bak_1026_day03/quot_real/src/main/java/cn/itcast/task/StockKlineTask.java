package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.map.StockKlineMapFunction;
import cn.itcast.function.sink.MySQLSink;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

/**
 * 个股K线Task(日、周、月)
 */
public class StockKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

        /**
         * 开发步骤：
         * 1.新建侧边流分支（周、月）
         * 2.数据分组
         * 3.划分时间窗口
         * 4.数据处理
         * 5.分流、封装侧边流数据
         * 6.编写插入sql
         * 7.（日、周、月）K线数据写入
         * 数据转换
         * 数据分组
         * 数据写入mysql
         */
        /*//1.新建侧边流分支（周、月）
        OutputTag<StockBean> weekStock = new OutputTag<>("weekStock", TypeInformation.of(StockBean.class));
        OutputTag<StockBean> monthStock = new OutputTag<>("monthStock", TypeInformation.of(StockBean.class));
        //2.数据分组
        SingleOutputStreamOperator<StockBean> processData = waterData.keyBy(new KeyFunction())
                //3.划分时间窗口
                .timeWindow(Time.minutes(1))
                //4.数据处理
                .apply(new StockMinutesWindowFunction())
                //5.分流、封装侧边流数据
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        ctx.output(weekStock, value); //待插入周K线表
                        ctx.output(monthStock, value); //待插入月K线表
                        out.collect(value);
                    }
                });*/

        SingleOutputStreamOperator<StockBean> stockBeanDS = waterData
                //1.分组
                .keyBy(new KeyFunction())
                //2.划分时间窗口
                .timeWindow(Time.minutes(1))
                //3.数据处理
                .apply(new StockMinutesWindowFunction());

        //4.编写插入sql
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //5.K线数据转换并写入MySQL
        //日K：1，周K：2，月K：3
        //日K
        stockBeanDS.map(new StockKlineMapFunction(KlineType.DAY_K.getType(), KlineType.DAY_K.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_STOCK_SQL_DAY_TABLE)))
                /*.setParallelism(2)*/;

        //周K
        stockBeanDS.map(new StockKlineMapFunction(KlineType.WEEK_K.getType(), KlineType.WEEK_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_STOCK_SQL_WEEK_TABLE)))
                /*.setParallelism(2)*/;

        //月K
        stockBeanDS.map(new StockKlineMapFunction(KlineType.MONTH_K.getType(), KlineType.MONTH_K.getFirstTxDateType()))
                .keyBy(value -> value.getField(2))
                .addSink(new MySQLSink(String.format(sql, QuotConfig.MYSQL_STOCK_SQL_MONTH_TABLE)))
                /*.setParallelism(2)*/;
    }
}
