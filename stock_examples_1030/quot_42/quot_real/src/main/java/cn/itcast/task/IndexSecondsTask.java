package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.sink.HBaseSink;
import cn.itcast.function.window.IndexPutHBaseWindowFunction;
import cn.itcast.function.window.IndexSecondsWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author itcast
 * Date 2020/10/26 15:33
 * Desc 指数秒级行情数据业务处理
 */
public class IndexSecondsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //TODO 1.数据分组waterData.keyBy(new KeyFunction())
        waterData.keyBy(CleanBean::getSecCode)//按照指数代码分组"secCode"
        //TODO 2.划分时间窗口.timeWindow(Time.seconds(5))//5s一个窗口，不重叠的时间窗口
        .timeWindow(Time.seconds(5))
        //TODO 3.秒级窗口函数业务处理.apply(new IndexSecondsWindowFunction())
        //如果是简单业务可以直接使用sum/min/max...
        //如果是复杂业务可以apply或process
        //下面的apply完成了将最近5s内最新的CleanBean封装为了IndexBean
        .apply(new IndexSecondsWindowFunction())
        //TODO 4.窗口数据合并.timeWindowAll(Time.seconds(5))
        .timeWindowAll(Time.seconds(5))// 把所有指数5s内最新的IndexBean合到一个窗口
        //TODO 5.封装List<Put>批写入对象.apply(new IndexPutHBaseWindowFunction())
        .apply(new IndexPutHBaseWindowFunction())
        //代码走到这里数据已经封装到List<Put>中的
        //TODO 6.数据写入.addSink(new HBaseSink(QuotConfig.INDEX_HBASE_TABLE_NAME));
       .addSink(new HBaseSink(QuotConfig.INDEX_HBASE_TABLE_NAME));
    }
}
