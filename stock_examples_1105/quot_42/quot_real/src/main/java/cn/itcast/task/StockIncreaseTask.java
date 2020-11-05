package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncreaseBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.window.StockMinutesIncreaseWindowFunction;
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
 * Date 2020/10/27 10:12
 * Desc 个股涨跌幅核心业务类
 * 注意:
 * 1.窗口也是60s
 * 2.计算结果存入Druid(kafka,分/不分主题存都行)
 */
public class StockIncreaseTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //0.创建kafka生产者对象
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        FlinkKafkaProducer011<String> kafkaSink = new FlinkKafkaProducer011<>(QuotConfig.STOCK_INCREASE_TOPIC,  new SimpleStringSchema(),  props);

        //1.数据分组 .keyBy(CleanBean::getSecCode)
        waterData.keyBy(CleanBean::getSecCode)
        //2.划分时间窗口.timeWindow(Time.seconds(60))
        .timeWindow(Time.seconds(60))
        //3.分时数据处理 .apply(new StockMinutesIncreaseWindowFunction())
        .apply(new StockMinutesIncreaseWindowFunction())
        //4.数据转为json
        .map(new MapFunction<StockIncreaseBean, String>() {
            @Override
            public String map(StockIncreaseBean stockIncreaseBean) throws Exception {
                return JSON.toJSONString(stockIncreaseBean);
            }
        })
        //5.数据存储到Kafka(Druid)
        .addSink(kafkaSink);

        /*
        1.准备stock-increase主题
        2.启动Druid摄取该主题并生成该数据源stock_stream_increase(也就是把它变为running)
        3.启动程序
        4.查询Druid看有没有最新的数据
         */
    }
}
