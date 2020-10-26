package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncreaseBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
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
 * 涨跌幅行情
 */
public class StockIncreaseTask implements ProcessDataInterface {
    /*
    1.涨跌幅行情：也是分时行情的一种，一分钟一条数据
    2.数据也是实时查询的，数据是存储在druid
     */

    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.创建bean对象
         * 4.分时数据处理（新建分时窗口函数）
         * 5.数据转换成字符串
         * 6.数据存储(单表)
         */
        //创建kafka生产者对象
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.STOCK_INCREASE_TOPIC, new SimpleStringSchema(), properties);

        //1.数据分组
        waterData.keyBy(new KeyFunction())
                //2.划分时间窗口
                .timeWindow(Time.seconds(60))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new StockMinutesIncreaseWindowFunction())
                //5.数据转换成字符串
                .map(new MapFunction<StockIncreaseBean, String>() {
                    @Override
                    public String map(StockIncreaseBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                 //6.数据存储(单表)
                }).addSink(kafkaProducer);
    }
}
