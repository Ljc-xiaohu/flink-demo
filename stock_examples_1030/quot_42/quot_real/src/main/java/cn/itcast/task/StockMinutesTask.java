package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
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
 * Author itcast
 * Date 2020/10/26 17:09
 * Desc
 * 分时/分级行情核心业务处理类
 */
public class StockMinutesTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤:
        //TODO 1.定义侧边流-注意:后续分时数据处理结果要存到Druid其实就存到Kafka,为了减少Kafka的压力,所以写到Kafka的不同主题,也便于后续按沪市深市进行数据分析
        OutputTag<StockBean> sse = new OutputTag<>("sse", TypeInformation.of(StockBean.class));
        OutputTag<StockBean> szse = new OutputTag<>("szse", TypeInformation.of(StockBean.class));

        //TODO 2.数据分组.keyBy(CleanBean::getSecCode)
        SingleOutputStreamOperator<StockBean> precessDS = waterData.keyBy(CleanBean::getSecCode)//按照股票代码分组
                //TODO 3.划分时间窗口.timeWindow(Time.minutes(1))
                .timeWindow(Time.minutes(1))
                //TODO 4.分时数据处理.apply(new StockMinutesWindowFunction())获取分时成交量/金额(当前窗口的总成交金额/量 - 上一窗口的总成交金额/量 --->使用MapState)
                .apply(new StockMinutesWindowFunction())
                //TODO 5.数据分流 .process(new ProcessFunction<StockBean, StockBean>() {
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean stockBean, Context context, Collector<StockBean> collector) throws Exception {
                        if (stockBean.getSource().equals("sse")) {
                            //沪市
                            context.output(sse, stockBean);
                        }else{
                            //System.out.println("数据收集到深市");
                            //深市
                            context.output(szse, stockBean);
                        }
                    }
                });
        //TODO 6.获取侧边流数据
        DataStream<StockBean> sseDS = precessDS.getSideOutput(sse);
        DataStream<StockBean> szseDS = precessDS.getSideOutput(szse);

        //TODO 7.数据转换为json
        SingleOutputStreamOperator<String> sseJsonDS = sseDS.map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean stockBean) throws Exception {
                return JSON.toJSONString(stockBean);
            }
        });
        SingleOutputStreamOperator<String> szseJsonDS = szseDS.map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean stockBean) throws Exception {
                return JSON.toJSONString(stockBean);
            }
        });

        //TODO 8.写入kafka不同主题
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        FlinkKafkaProducer011<String> sseKafkaSink = new FlinkKafkaProducer011<>(QuotConfig.SSE_STOCK_TOPIC,  new SimpleStringSchema(),  props);
        FlinkKafkaProducer011<String> szseKafkaSink = new FlinkKafkaProducer011<>(QuotConfig.SZSE_STOCK_TOPIC,  new SimpleStringSchema(),  props);
        //sseJsonDS.print("沪市");
        //szseJsonDS.print("深市");

        sseJsonDS.addSink(sseKafkaSink);
        szseJsonDS.addSink(szseKafkaSink);
    }
}
