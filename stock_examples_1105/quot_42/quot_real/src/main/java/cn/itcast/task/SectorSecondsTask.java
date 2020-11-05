package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.sink.HBaseSink;
import cn.itcast.function.window.SectorPutHBaseWindowFunction;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockSecondsWindowFunctionWithState;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author itcast
 * Date 2020/10/27 14:47
 * Desc 行业板块秒级行情业务核心类
 */
public class SectorSecondsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //TODO 1.数据分组.keyBy(CleanBean::getSecCode)
        waterData.keyBy(CleanBean::getSecCode)
        //TODO 2.划分时间窗口.timeWindow(Time.seconds(5))
        .timeWindow(Time.seconds(5))
        //TODO 3.个股数据处理.apply(new StockSecondsWindowFunctionWithState())(因为板块数据来自于个股,且需要计算成交量/金额)
        //注意:
        // 尽管现在是在做行业板块业务,但是板块数据由个股构成,
        // 所以首先需要把当前窗口最新的CleanBean转为StockBean,
        // 且行业板块秒级行情业务需要计算秒级成交量/金额!(也就是最近5s成交量/金额)
        .apply(new StockSecondsWindowFunctionWithState())
        //TODO 4.数据汇总.timeWindowAll(Time.seconds(5))数据汇总在一起,包含单一交易所的数据--沪市
        .timeWindowAll(Time.seconds(5))
        //TODO 5.行业板块秒级行情数据处理.apply(new SectorWindowFunction())板块业务核心代码开发
        .apply(new SectorWindowFunction())
        //TODO 6.数据汇总 .timeWindowAll(Time.seconds(5))
        .timeWindowAll(Time.seconds(5))
        //TODO 7.数据封装为List<Put>调用.apply(new SectorPutHBaseWindowFunction())
        .apply(new SectorPutHBaseWindowFunction())
        //TODO 8.数据写入数据写入HBase调用.addSink(new HBaseSink(QuotConfig.SECTOR_HBASE_TABLE_NAME));
        .addSink(new HBaseSink(QuotConfig.SECTOR_HBASE_TABLE_NAME));
    }
}
