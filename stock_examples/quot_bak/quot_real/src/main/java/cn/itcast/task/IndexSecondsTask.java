package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.window.IndexPutHBaseWindowFunction;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.window.IndexSecondsWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import cn.itcast.function.sink.HBaseSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 指数行情
 */
public class IndexSecondsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
         * 4.数据写入操作
         *   * 封装ListPuts
         *   * 数据写入
         */
        //1.数据分组
        waterData.keyBy(new KeyFunction())
                //2.划分时间窗口
                .timeWindow(Time.seconds(5))
                //3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
                .apply(new IndexSecondsWindowFunction())
                .timeWindowAll(Time.seconds(5))
                //封装ListPuts
                .apply(new IndexPutHBaseWindowFunction())
                //数据写入
                .addSink(new HBaseSink(QuotConfig.INDEX_HBASE_TABLE_NAME));
    }
}
