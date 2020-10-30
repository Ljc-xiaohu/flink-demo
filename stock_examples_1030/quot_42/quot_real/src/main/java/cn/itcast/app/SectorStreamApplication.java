package cn.itcast.app;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.map.SseAvro2CleanBeanMapFunction;
import cn.itcast.task.SectorKlineTask;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
        //TODO 表示该代码未完成,方便后续通过TODO标签查找到,我们这里使用TODO还有一个作用:方便区分步骤
        //TODO 1.创建流处理环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置事件时间、并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);//学习测试设置为1即可
        //TODO 3.设置Checkpoint机制-学习时开发时可以注掉,上线再打开
        /*//===========Checkpoint参数设置====
        //===========类型1:必须参数=============
        //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
        env.enableCheckpointing(1000);
        //设置State状态存储介质
        if(SystemUtils.IS_OS_WINDOWS){
            env.setStateBackend(new FsStateBackend("file:///D:\\data\\ckp"));
        }else{
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint"));
        }
        //===========类型2:建议参数===========
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1*/

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
        //new SectorMinutesBackupTask().process(waterData);
        //TODO -4.行业板块K线(日、周、月)
        new SectorKlineTask().process(waterData);

        //TODO 12.触发执行
        env.execute();

    }
}
