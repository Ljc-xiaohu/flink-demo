package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.sink.HBaseSink;
import cn.itcast.function.window.StockPutHBaseWindowFunction;
import cn.itcast.function.window.StockSecondsWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 个股秒级任务处理
 */
public class StockSecondsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.新建个股数据写入bean对象
         * 4.秒级窗口函数业务处理
         * 5.数据写入操作
         *   * 封装ListPuts
         *   * 数据写入
         */
        //1.数据分组
        //waterData.keyBy(new KeyFunction())
        waterData.keyBy(CleanBean::getSecCode)
                .timeWindow(Time.seconds(5))//5s一个窗口，不重叠的时间窗口
                //4.秒级窗口函数业务处理
                .apply(new StockSecondsWindowFunction()) //获取秒级结果数据
                // 5.数据写入操作
                .timeWindowAll(Time.seconds(5))
                //封装ListPuts
                .apply(new StockPutHBaseWindowFunction())
                //数据写入HBase,通用性写法
                .addSink(new HBaseSink(QuotConfig.STOCK_HBASE_TABLE_NAME));//config.getProperty("stock.hbase.table.name")
    }
}
