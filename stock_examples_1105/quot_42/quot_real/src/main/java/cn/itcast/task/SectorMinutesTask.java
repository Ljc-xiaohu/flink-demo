package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * Author itcast
 * Date 2020/10/27 17:14
 * Desc
 * 行业板块分时行情业务处理核心类
 */
public class SectorMinutesTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //0.创建kafka生产者对象
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.SSE_SECTOR_TOPIC, new SimpleStringSchema(), properties);

        //1.数据分组.keyBy(CleanBean::getSecCode)
        waterData.keyBy(CleanBean::getSecCode)
        //2.划分个股时间窗口.timeWindow(Time.seconds(60))
        .timeWindow(Time.seconds(60))
        //3.个股分时数据处理.apply(new StockMinutesWindowFunction())
        .apply(new StockMinutesWindowFunction())
        //4.划分板块时间窗口.timeWindowAll(Time.seconds(60))
        .timeWindowAll(Time.seconds(60))
        //5.板块分时数据处理.apply(new SectorWindowFunction())
        //注意:板块行情不管是秒级还是分级/分时,里面的SectorBean的处理都一样,所以可以复用SectorWindowFunction
        .apply(new SectorWindowFunction())
        //6.数据转换成json字符串
        .map(new MapFunction<SectorBean, String>() {
            @Override
            public String map(SectorBean sectorBean) throws Exception {
                return JSON.toJSONString(sectorBean);
            }
        })
        //7.数据写入kafka
        .addSink(sseKafkaProducer);
    }
}
