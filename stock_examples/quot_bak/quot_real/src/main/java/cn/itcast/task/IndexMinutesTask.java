package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.window.IndexMinutesWindowFunction;
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
 * 指数分时行情
 */
public class IndexMinutesTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //1.定义侧边流,接收深市指数行情数据
        OutputTag<IndexBean> sse = new OutputTag<>("sse", TypeInformation.of(IndexBean.class));
        OutputTag<IndexBean> szse = new OutputTag<>("szse", TypeInformation.of(IndexBean.class));

        //2.数据分组
        SingleOutputStreamOperator<IndexBean> processDS = waterData
                .keyBy(new KeyFunction())
                //3.划分时间窗口
                .timeWindow(Time.minutes(1))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new IndexMinutesWindowFunction())
                //5.数据分流
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean value, Context ctx, Collector<IndexBean> collector) throws Exception {
                        if (value.getSource().equals(QuotConfig.SSE_TOPIC)) {
                            //沪市
                            ctx.output(sse, value);
                        } else {
                            //深市
                            ctx.output(szse, value);
                        }
                    }
                });

        DataStream<IndexBean> sseDS = processDS.getSideOutput(sse);
        DataStream<IndexBean> szseDS = processDS.getSideOutput(szse);

        //6.数据分流转换
        SingleOutputStreamOperator<String> sseJsonStr = sseDS.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        SingleOutputStreamOperator<String> szseJsonStr = szseDS.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        //7.写入kafka不同主题
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.SSE_INDEX_TOPIC, new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.SZSE_INDEX_TOPIC, new SimpleStringSchema(), properties);

        sseJsonStr.addSink(sseKafkaProducer);
        szseJsonStr.addSink(szseKafkaProducer);

        /*SingleOutputStreamOperator<IndexBean> processData = waterData.keyBy(new KeyFunction())
                //3.划分时间窗口
                .timeWindow(Time.seconds(60))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new IndexMinutesWindowFunction())
                //6.数据分流转换
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean value, Context ctx, Collector<IndexBean> out) throws Exception {
                        if (value.getSource().equals(QuotConfig.config.getProperty("sse.topic"))) {
                            out.collect(value);
                        } else {
                            ctx.output(indexOpt, value);
                        }
                    }
                });


        //创建kafka生产者对象
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",QuotConfig.config.getProperty("bootstrap.servers"));
        FlinkKafkaProducer011<String> sseKafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.index.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.index.topic"), new SimpleStringSchema(), properties);

        //7.数据分流转换
        //沪市
        processData.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).addSink(sseKafkaPro);

        //深市
        processData.getSideOutput(indexOpt).map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).addSink(szseKafkaPro);*/
    }
}
