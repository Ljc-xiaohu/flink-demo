package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.sink.HBaseSink;
import cn.itcast.function.window.SectorPutHBaseWindowFunction;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockSecondsWindowFunctionWithState;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 板块秒级行情
 */
public class SectorSecondsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组keyBy(new KeyFunction())
         * 2.划分时间窗口timeWindow(Time.seconds(5))
         * 3.个股数据处理(因为板块数据来自于个股,且需要计算成交量/金额)
         * 4.数据汇总timeWindowAll(Time.seconds(5))数据汇总在一起,包含单一交易所的数据
         * 5.板块秒级行情数据处理-板块业务核心代码开发
         * 6.数据汇总timeWindowAll(Time.seconds(5))
         * 7.封装ListPuts
         * 8.数据写入
         */
        //1.数据分组
        waterData.keyBy(new KeyFunction())
                //2.划分时间窗口.timeWindow(Time.seconds(5))
                .timeWindow(Time.seconds(5))
                //3.个股数据处理(因为板块数据来自于个股,且需要计算成交量/金额)
                .apply(new StockSecondsWindowFunctionWithState())
                //4.数据汇总timeWindowAll(Time.seconds(5))数据汇总在一起,包含单一交易所的数据
                .timeWindowAll(Time.seconds(5))
                //5.板块秒级行情数据处理-板块业务核心代码开发
                .apply(new SectorWindowFunction())
                //6.数据汇总timeWindowAll(Time.seconds(5))
                .timeWindowAll(Time.seconds(5))
                //7.封装ListPuts
                .apply(new SectorPutHBaseWindowFunction())
                //8.数据写入HBase
                .addSink(new HBaseSink(QuotConfig.SECTOR_HBASE_TABLE_NAME));
    }
}
