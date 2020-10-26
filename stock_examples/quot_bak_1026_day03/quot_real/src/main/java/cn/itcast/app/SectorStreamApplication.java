package cn.itcast.app;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.map.SseMapFunction;
import cn.itcast.task.SectorKlineTask;
import cn.itcast.task.SectorMinutesBackupTask;
import cn.itcast.task.SectorMinutesTask;
import cn.itcast.task.SectorSecondsTask;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 板块行情：秒级、分时、数据备份、K线
 */
public class SectorStreamApplication {
    public static void main(String[] args) throws Exception {
        //1.创建流处理环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置事件时间、并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);//开发时为了便于观察和调试所以设置为1
        //4.设置检查点机制-开发时可以注掉
        /*//=================建议必须设置的===================
        //设置Checkpoint-State的状态后端为FsStateBackend,本地测试时使用本地路径,集群测试时使用传入的HDFS的路径
        if(SystemUtils.IS_OS_WINDOWS){
            env.setStateBackend(new FsStateBackend("file:///D:/data/ckp"));
        }else{
            env.setStateBackend(new FsStateBackend(QuotConfig.config.getProperty("stock.sec.hdfs.path")));
        }
        //设置Checkpointing时间间隔为5000ms,意思是做 2 个 Checkpoint 的间隔为5000ms。Checkpoint 做的越频繁，恢复数据时就越简单，同时 Checkpoint 相应的也会有一些IO消耗。
        env.enableCheckpointing(5000);//(默认情况下如果不设置时间checkpoint是没有开启的)
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //=================直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认),注意:得需要外部支持,如Source和Sink的支持
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1*/

        //5.设置重启机制
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //6.整合Kafka(需要创建反序列化类)
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        props.setProperty("group.id", QuotConfig.GROUP_ID);
        props.setProperty("flink.partition-discovery.interval-millis","5000");//开一个后台线程每隔5s检查Kafka的分区状态
        //消费沪市行情
        FlinkKafkaConsumer011<SseAvro> sseKafkaConsumer = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.SSE_TOPIC, new AvroDeserializeSchema(QuotConfig.SSE_TOPIC), props);
        sseKafkaConsumer.setStartFromEarliest();
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafkaConsumer);
        //sseSource.print("沪市：");

        // 7.数据过滤（时间和null字段）
        //沪市过滤
        SingleOutputStreamOperator<SseAvro> sseFilter = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro value) throws Exception {
                //时间过滤和数据是否为0过滤
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });

        //8.数据转换
        DataStream<CleanBean> CleanBeanDS = sseFilter.map(new SseMapFunction());

        //9.过滤个股数据
        SingleOutputStreamOperator<CleanBean> filterData = CleanBeanDS.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                return QuotUtil.isStock(value);
            }
        });
        //filterData.print("个股数据：");

        //延时时间
        Long delayTime = Long.valueOf(QuotConfig.DELAY_TIME);
        //10.设置水位线
        DataStream<CleanBean> waterData = filterData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(delayTime)) {
            @Override
            public long extractTimestamp(CleanBean cleanBean) {
                return cleanBean.getEventTime();
            }
        });
        //waterData.print("水位线：");

        //业务开发
        /**
         * -1.板块秒级行情
         * -2.板块分时行情
         * -3.板块分时行情备份至HDFS
         * -4.板块K线
         */
        //-1.板块秒级行情
        new SectorSecondsTask().process(waterData);
        //-2.分时行情
        new SectorMinutesTask().process(waterData);
        //-3.分时行情备份至HDFS
        new SectorMinutesBackupTask().process(waterData);
        //-4.板块K线
        new SectorKlineTask().process(waterData);

        //触发执行
        env.execute("sector stream");
    }

}
