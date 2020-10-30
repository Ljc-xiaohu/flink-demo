package cn.itcast.cep;

import cn.itcast.bean.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Date 2020/9/21
 * 需求：机架温控预警
 * 预警规则1：警告：某机架在10秒内连续两次上报的温度超过阈值
 * 预警规则2：报警：某机架在20秒内连续两次匹配警告，并且第二次的警告温度超过了第一次的警告温度就报警
 */
public class Demo9_RackWarn {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.source
        SingleOutputStreamOperator<TemperatureMonitoringEvent> source = env.addSource(new MonitoringEventSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TemperatureMonitoringEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(TemperatureMonitoringEvent element) {
                        return element.getTimestamp();
                    }
                });

        //3.transformation
        //预警规则1：警告：某机架在10秒内连续两次上报的温度超过阈值
        //-1.定义模式规则
        Pattern<TemperatureMonitoringEvent, TemperatureEventTemperature> patternWarn = Pattern
                .<TemperatureMonitoringEvent>begin("begin").subtype(TemperatureEventTemperature.class)
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

        //-2.将规则应用到数据流
        PatternStream<TemperatureMonitoringEvent> cepWarn = CEP.pattern(source.keyBy(TemperatureMonitoringEvent::getRackID), patternWarn);
        //-3.获取符合规则的数据返回TemperatureWarning警告对象
        SingleOutputStreamOperator<TemperatureWarning> warnData = cepWarn.select(new PatternSelectFunction<TemperatureMonitoringEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<TemperatureMonitoringEvent>> pattern) throws Exception {
                TemperatureEventTemperature begin = (TemperatureEventTemperature) pattern.get("begin").get(0);
                TemperatureEventTemperature end = (TemperatureEventTemperature) pattern.get("next").get(0);
                //返回机架id和触发警告的平均温度
                return new TemperatureWarning(end.getRackID(), (begin.getTemperature() + end.getTemperature()) / 2);
            }
        });
        //4.sink
        warnData.print("规则1警告数据：");

        //预警规则2：报警：某机架在20秒内连续两次匹配警告，并且第二次的温度超过了第一次的温度就告警
        //-1.定义模式规则
        Pattern<TemperatureWarning, TemperatureWarning> alertPattern = Pattern
                //直接使用上面定义过的规则/模式
                .<TemperatureWarning>begin("begin").next("next").within(Time.seconds(20));
        //-2.将规则应用到数据流
        PatternStream<TemperatureWarning> cepAlert = CEP.pattern(warnData.keyBy(TemperatureWarning::getRackID), alertPattern);
        //-3.获取符合规则的数据返回TemperatureAlert报警对象
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

        //4.sink
        result.print("规则2报警数据==>：");

        //5.execute
        env.execute();
    }

    /**
     * 自定义source模拟生成机架id、温度、电压等数据。
     */
    private static class MonitoringEventSource extends RichParallelSourceFunction<TemperatureMonitoringEvent> {
        private boolean flag = true;
        private final double temperatureRatio = 0.5;//温度阈值
        private final double powerStd = 100;//标准功率
        private final double powerMean = 10; //平均功率
        private final double temperatureStd = 80;//标准温度
        private final double temperatureMean = 20;//平均温度

        public void run(SourceContext<TemperatureMonitoringEvent> sourceContext) throws Exception {
            while (flag) {
                TemperatureMonitoringEvent temperatureMonitoringEvent;
                //生成随机数的对象
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                int rackId = random.nextInt(2);
                //如果生成的随机温度大于温度阈值，那么就是过热
                if (random.nextDouble() >= temperatureRatio) {
                    //用Random类中的nextGaussian()方法，可以产生服从高斯分布的随机数，高斯分布即标准正态分布，均值为0，方差为1。
                    double power = random.nextGaussian() * powerStd + powerMean;
                    temperatureMonitoringEvent = new TemperaturePowerEventTemperature(rackId, power, System.currentTimeMillis());
                } else {
                    double temperature = random.nextGaussian() * temperatureStd + temperatureMean;
                    temperatureMonitoringEvent = new TemperatureEventTemperature(rackId, temperature, System.currentTimeMillis());
                }
                //System.out.println("随机生成的数据:"+ temperatureMonitoringEvent);
                sourceContext.collect(temperatureMonitoringEvent);
                //Thread.sleep(1000);
            }
        }
        public void cancel() {
            flag = false;
        }
    }
}