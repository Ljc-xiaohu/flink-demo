package cn.itcast.app;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.map.SseMapFunction;
import cn.itcast.function.map.SzseMapFunction;
import cn.itcast.task.WarnAmplitudeTask;
import cn.itcast.task.WarnUpdownTask;
import cn.itcast.task.WarnTurnoverRateTask;
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
 * 行情预警业务：涨跌幅、振幅、换手率
 * 可以针对个股、指数和板块业务，
 * 功能较为重复，我们这里目前只进行个股行情分析和预警
 */
public class WarnStreamApplication {
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

        //6.整合Kafka(新建反序列化类)
        //6.整合Kafka(需要创建反序列化类)
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", QuotConfig.BOOTSTRAP_SERVERS);
        props.setProperty("group.id", QuotConfig.GROUP_ID);
        props.setProperty("flink.partition-discovery.interval-millis","5000");//开一个后台线程每隔5s检查Kafka的分区状态

        //沪市
        FlinkKafkaConsumer011<SseAvro> sseKafkaConsumer = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.SSE_TOPIC, new AvroDeserializeSchema(QuotConfig.SSE_TOPIC), props);
        sseKafkaConsumer.setStartFromEarliest();
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafkaConsumer);
        //sseSource.print("沪市：");
        //深市
        FlinkKafkaConsumer011<SzseAvro> szseKafkaConsumer = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.SZSE_TOPIC, new AvroDeserializeSchema(QuotConfig.SZSE_TOPIC), props);
        szseKafkaConsumer.setStartFromEarliest();
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafkaConsumer);
        //szseSource.print("深市：");

        //8.数据过滤:校验时间和字段非空null
        //沪市过滤
        SingleOutputStreamOperator<SseAvro> sseFilter = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro value) throws Exception {
                //时间过滤和数据是否为0过滤
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });

        //深市过滤
        SingleOutputStreamOperator<SzseAvro> szseFilter = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro value) throws Exception {
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });

        //9.数据转换为CleanBean并合并
        SingleOutputStreamOperator<CleanBean> CleanBeanDS1 = sseFilter.map(new SseMapFunction());
        SingleOutputStreamOperator<CleanBean> CleanBeanDS2 = szseFilter.map(new SzseMapFunction());
        DataStream<CleanBean> unionData = CleanBeanDS1.union(CleanBeanDS2);
        //unionData.print("合并: ");

        //10.过滤个股数据
        SingleOutputStreamOperator<CleanBean> filterData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                return QuotUtil.isStock(value);
            }
        });
        //filterData.print("个股数据：");

        //延时时间
        Long delayTime = Long.valueOf(QuotConfig.DELAY_TIME);
        //11.设置水位线
        DataStream<CleanBean> waterData = filterData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(delayTime)) {
            @Override
            public long extractTimestamp(CleanBean cleanBean) {
                return cleanBean.getEventTime();
            }
        });
        //waterData.print("水位线：");

        //12.业务开发
        /**
         * -1.振幅
         * -2.涨跌幅
         * -3.换手率
         */
        //-1.振幅
        new WarnAmplitudeTask().process(waterData,env);
        //-2.涨跌幅
        new WarnUpdownTask().process(waterData);
        //-3.换手率
        new WarnTurnoverRateTask().process(waterData);

        //13.触发执行
        env.execute("warn stream");
    }
}
