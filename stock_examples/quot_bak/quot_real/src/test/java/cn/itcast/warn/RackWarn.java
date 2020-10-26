package cn.itcast.warn;

import cn.itcast.bean.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @Date 2020/9/21
 * 机架温控预警
 */
public class RackWarn {

    /**
     * 需求：
     * 警告：某机架在10秒内连续两次上报的温度超过阈值 ：对应一个预警规则
     * 报警：某机架在20秒内连续两次匹配警告，并且第二次的温度超过了第一次的温度就告警 ：对应第二个预警规则
     */
    public static void main(String[] args) throws Exception {

        /**
         * 1.获取流处理执行环境
         * 2.设置事件时间
         * 3.加载数据源，接收监视数据,设置提取时间
         * 4.定义匹配模式，设置预警匹配规则，警告：10s内连续两次超过阀值
         * 5.生成匹配模式流（分组）
         * 6.数据处理,生成警告数据
         * 7.二次定义匹配模式，告警：20s内连续两次匹配警告
         * 8.二次生成匹配模式流（分组）
         * 9.数据处理生成告警信息flatSelect，返回类型
         * 10.数据打印(警告和告警)
         * 11.触发执行
         */
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(12);
        //2.设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.加载数据源，接收监视数据,设置提取时间
        SingleOutputStreamOperator<TemperatureMonitoringEvent> source = env.addSource(new MonitoringEventSource()).assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
        //4.定义匹配模式，设置预警匹配规则，
        // 警告：10s内连续两次超过阀值
        Pattern<TemperatureMonitoringEvent, TemperatureEventTemperature> patternWarn = Pattern.<TemperatureMonitoringEvent>begin("begin").subtype(TemperatureEventTemperature.class)
                .where(new SimpleCondition<TemperatureEventTemperature>() {
                    @Override
                    public boolean filter(TemperatureEventTemperature value) throws Exception {
                        return value.getTemperature() > 100;
                    }
                }).next("next").subtype(TemperatureEventTemperature.class)
                .where(new SimpleCondition<TemperatureEventTemperature>() {
                    @Override
                    public boolean filter(TemperatureEventTemperature value) throws Exception {
                        return value.getTemperature() > 100;
                    }
                }).within(Time.seconds(10));

        //5.生成匹配模式流（分组）
        PatternStream<TemperatureMonitoringEvent> cepWarn = CEP.pattern(source.keyBy(TemperatureMonitoringEvent::getRackID), patternWarn);
        //6.数据处理,生成警告数据
        SingleOutputStreamOperator<TemperatureWarning> warnData = cepWarn.select(new PatternSelectFunction<TemperatureMonitoringEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<TemperatureMonitoringEvent>> pattern) throws Exception {
                TemperatureEventTemperature begin = (TemperatureEventTemperature) pattern.get("begin").get(0);
                TemperatureEventTemperature end = (TemperatureEventTemperature) pattern.get("next").get(0);

                return new TemperatureWarning(end.getRackID(), (begin.getTemperature() + end.getTemperature()) / 2);
            }
        });
        warnData.print("警告数据：");

        //7.二次定义匹配模式，告警：20s内连续两次匹配警告
        Pattern<TemperatureWarning, TemperatureWarning> alertPattern = Pattern.<TemperatureWarning>begin("begin").next("next").within(Time.seconds(20));
        // 8.二次生成匹配模式流（分组）
        PatternStream<TemperatureWarning> cepAlert = CEP.pattern(warnData.keyBy(TemperatureWarning::getRackID), alertPattern);
        // 9.数据处理生成告警信息flatSelect，返回类型
        SingleOutputStreamOperator<TemperatureAlert> result = cepAlert.flatSelect(new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
            @Override
            public void flatSelect(Map<String, List<TemperatureWarning>> pattern, Collector<TemperatureAlert> out) throws Exception {
                TemperatureWarning begin = pattern.get("begin").get(0);
                TemperatureWarning end = pattern.get("next").get(0);
                if (end.getAverageTemperature() > begin.getAverageTemperature()) {
                    out.collect(new TemperatureAlert(begin.getRackID()));
                }
            }
        });

        // 10.数据打印(警告和告警)
        result.print("告警数据：");

        //11.触发执行
        env.execute();
    }

    /**
     * 我们通过自定义的source来模拟生成MonitoringEvent数据。
     */
    private static class MonitoringEventSource extends RichParallelSourceFunction<TemperatureMonitoringEvent> {
        private boolean running = true;
        private final int maxRackId = 10;//最大机架id
        private final long pause = 100; //暂停
        private final double temperatureRatio = 0.5;//温度阈值
        private final double powerStd = 100;//标准功率
        private final double powerMean = 10; //平均功率
        private final double temperatureStd = 80;//标准温度
        private final double temperatureMean = 20;//平均温度
        private int shard;//碎片
        private int offset;//偏移量

        @Override
        public void open(Configuration configuration) {
            //numParallelSubtasks 表示总共并行的subtask 的个数，
            int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
            //获取当前子任务的索引
            int index = getRuntimeContext().getIndexOfThisSubtask();
            offset = (int) ((double) maxRackId / numberTasks * index);
            shard = (int) ((double) maxRackId / numberTasks * (index + 1)) - offset;
            System.out.println("numberTasks:" + numberTasks + ", index:" + index + ", offset:" + offset + ", shard:" + shard);
        }

        public void run(SourceContext<TemperatureMonitoringEvent> sourceContext) throws Exception {
            while (running) {
                //监视事件对象
                TemperatureMonitoringEvent temperatureMonitoringEvent;
                //多线程下生成随机数的对象
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                int rackId = random.nextInt(shard) + offset;
                //如果生成的随机温度大于温度阈值，那么就是过热
                if (random.nextDouble() >= temperatureRatio) {
                    //用Random类中的nextGaussian()方法，可以产生服从高斯分布的随机数，高斯分布即标准正态分布，均值为0，方差为1。
                    double power = random.nextGaussian() * powerStd + powerMean;
                    temperatureMonitoringEvent = new TemperaturePowerEventTemperature(rackId, power, System.currentTimeMillis());
                } else {
                    double temperature = random.nextGaussian() * temperatureStd + temperatureMean;
                    temperatureMonitoringEvent = new TemperatureEventTemperature(rackId, temperature, System.currentTimeMillis());
                }
                sourceContext.collect(temperatureMonitoringEvent);
                Thread.sleep(pause);
            }
        }

        //volatile

        /**
         * 1：保证可见性
         * 2：不保证原子性
         * 3：禁止指令重排
         * JVM：主内存、工作内存
         */
        public void cancel() {
            running = false;
        }
    }
}
