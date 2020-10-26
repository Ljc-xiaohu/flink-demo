package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * 个股分时行情
 */
public class StockMinutesTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //-1.分时行情时间窗口60s
        //-2.获取分时成交量/金额（当前窗口的总成交金额/量 - 上一窗口的总成交金额/量 -->MapState）
        //-3.先将数据包装成json字符串写入Kafka ->再写入Druid

        //1.定义侧边流
        OutputTag<StockBean> sse = new OutputTag<>("sse", TypeInformation.of(StockBean.class));
        OutputTag<StockBean> szse = new OutputTag<>("szse", TypeInformation.of(StockBean.class));
        //2.数据分组
        SingleOutputStreamOperator<StockBean> processDS = waterData
                .keyBy(new KeyFunction())
                //3.划分时间窗口
                .timeWindow(Time.minutes(1))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new StockMinutesWindowFunction())
                //5.数据分流
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        if (value.getSource().equals(QuotConfig.SSE_TOPIC)) {
                            //沪市
                            ctx.output(sse, value);
                        } else {
                            //深市
                            ctx.output(szse, value);
                        }
                    }
                });

        DataStream<StockBean> sseDS = processDS.getSideOutput(sse);
        DataStream<StockBean> szseDS = processDS.getSideOutput(szse);

        // 6.数据分流转换
        SingleOutputStreamOperator<String> sseJsonStr = sseDS.map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean value) throws Exception {
                /*if(value.getSecCode().equals("")){
                }*/
                return JSON.toJSONString(value);
            }
        });

        SingleOutputStreamOperator<String> szseJsonStr = szseDS.map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        //7.写入kafka不同主题
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",QuotConfig.BOOTSTRAP_SERVERS);
        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.SSE_STOCK_TOPIC, new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.SZSE_STOCK_TOPIC, new SimpleStringSchema(), properties);

        sseJsonStr.addSink(sseKafkaProducer);
        szseJsonStr.addSink(szseKafkaProducer);
    }
}
