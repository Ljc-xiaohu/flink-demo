package cn.itcast.app;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.map.SseAvro2CleanBeanMapFunction;
import cn.itcast.task.SectorKlineTask;
import cn.itcast.task.SectorMinutesBackupTask;
import cn.itcast.util.FlinkUitl;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Author itcast
 * Date 2020/10/26 11:45
 * Desc
 * 行业板块行情处理业务：秒级、分时、数据备份、K线
 * 处理程序入口类
 */
public class SectorStreamApplication {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = FlinkUitl.init(args);

        //TODO 4.设置重启机制-学习时开发时可以注掉,上线再打开
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        //TODO 5.整合Kafka(需要创建反序列化类)
        Properties props  = new Properties();
        props.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        props.setProperty("group.id", QuotConfig.GROUP_ID);
        //props.setProperty("auto.offset.reset","latest");
        props.setProperty("flink.partition-discovery.interval-millis","5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况

        //TODO 6.消费沪市
        FlinkKafkaConsumer011<SseAvro> sseKafkaSource = new FlinkKafkaConsumer011<>(QuotConfig.SSE_TOPIC, new AvroDeserializeSchema(QuotConfig.SSE_TOPIC), props);
        sseKafkaSource.setStartFromEarliest();//直接每次都从最开始消费,方便学习测试,就不用每次再去运行sse-server.jar....
        //开发时下面的打开
        //sseKafkaSource.setStartFromGroupOffsets();//从记录的offset开始消费,如果没有从"auto.offset.reset","latest"
        //sseKafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<SseAvro> sseDS = env.addSource(sseKafkaSource);

        //TODO 7.数据过滤:校验时间和字段非空null,保证数据是交易时间内的,且是高开低收不为0的正常/合法数据
        SingleOutputStreamOperator<SseAvro> sseFilterDS = sseDS.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro sseAvro) throws Exception {
                return QuotUtil.checkTime(sseAvro) && QuotUtil.checkData(sseAvro);
            }
        });

        //代码能走到这里说明是合法数据!
        //TODO 8.数据转换为CleanBean
        SingleOutputStreamOperator<CleanBean> sseCleanBeanDS = sseFilterDS.map(new SseAvro2CleanBeanMapFunction());
        //sseCleanBeanDS.print("转换后的数据:");

        //TODO 9.过滤个股数据
        SingleOutputStreamOperator<CleanBean> stockDS = sseCleanBeanDS.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean cleanBean) throws Exception {
                return QuotUtil.isStock(cleanBean);
            }
        });
        //stockDS.print("个股:");

        //TODO 10.设置水位线
        //告诉Flink要基于事件时间进行处理
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//前面已经设置过了
        //告诉Flink每隔多久生成Watermaker
        env.getConfig().setAutoWatermarkInterval(200);//默认就是200ms
        //告诉Flink哪一列是事件时间,已经最大允许的延迟时间是多久
        //Watermaker = 事件时间 - 最大允许的延迟时间或乱序时间
        Long delayTime = Long.parseLong(QuotConfig.DELAY_TIME); //也就是2s
        SingleOutputStreamOperator<CleanBean> waterData = stockDS.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(delayTime)) {
                    @Override
                    public long extractTimestamp(CleanBean cleanBean) {
                        return cleanBean.getEventTime();
                    }
                }
        );
        //waterData.print("添加了Watermaker的数据:");

        //TODO 11.业务开发
        //TODO -1.行业板块秒级行情
        //new SectorSecondsTask().process(waterData);
        //TODO -2.行业板块分钟行情/分时行情
        //new SectorMinutesTask().process(waterData);
        //TODO -3.行业板块分钟行情/分时行情备份至HDFS
        new SectorMinutesBackupTask().process(waterData);
        //TODO -4.行业板块K线(日、周、月)
        new SectorKlineTask().process(waterData);

        //TODO 12.触发执行
        env.execute();

    }
}
