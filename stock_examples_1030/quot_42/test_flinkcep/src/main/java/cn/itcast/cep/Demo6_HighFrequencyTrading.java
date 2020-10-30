package cn.itcast.cep;

import cn.itcast.bean.TransactionEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 高频交易，找出活跃账户/交易活跃用户
 * 在这个场景中，我们模拟账户交易信息中，那些高频的转账支付信息，希望能发现其中的风险或者活跃的用户：
 * 需要找出那些 24 小时内至少 5 次有效交易的账户
 */
public class Demo6_HighFrequencyTrading {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.source
        //[TransactionEvent(accout=100XX, amount=100.0, timeStamp=1597905235000),
        // TransactionEvent(accout=100XX, amount=200.0, timeStamp=1597905236000),
        // TransactionEvent(accout=100XX, amount=300.0, timeStamp=1597905237000),
        // TransactionEvent(accout=100XX, amount=400.0, timeStamp=1597905238000),
        // TransactionEvent(accout=100XX, amount=500.0, timeStamp=1597905239000)]
        DataStream<TransactionEvent> source = env.fromElements(
                new TransactionEvent("100XX", 0.0D, 1597905234000L),//2020-08-20 14:33:54
                new TransactionEvent("100XX", 100.0D, 1597905235000L),//2020-08-20 14:33:55
                new TransactionEvent("100XX", 200.0D, 1597905236000L),//2020-08-20 14:33:56
                new TransactionEvent("100XX", 300.0D, 1597905237000L),//2020-08-20 14:33:57
                new TransactionEvent("100XX", 400.0D, 1597905238000L),//2020-08-20 14:33:58
                new TransactionEvent("100XX", 500.0D, 1597905239000L),//2020-08-20 14:33:59
                new TransactionEvent("101XX", 0.0D, 1597905240000L),//2020-08-20 14:34:00
                new TransactionEvent("101XX", 100.0D, 1597905241000L)//2020-08-20 14:34:01
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TransactionEvent>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(TransactionEvent element) {
                return element.getTimeStamp();
            }
        });
        //3.transformation
        //需要找出那些 24 小时内至少 5 次有效交易的账户
        //-1.定义模式规则
        Pattern<TransactionEvent, TransactionEvent> pattern = Pattern.<TransactionEvent>begin("start").where(
                new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent transactionEvent) {
                        return transactionEvent.getAmount() > 0;
                    }
                }
        ).timesOrMore(5)
         .within(Time.hours(24));
        //-2.将规则应用到数据流
        PatternStream<TransactionEvent> patternStream = CEP.pattern(source.keyBy(TransactionEvent::getAccout), pattern);
        //-3.获取符合规则的数据
        SingleOutputStreamOperator<Object> result = patternStream.select(new PatternSelectFunction<TransactionEvent, Object>() {
            @Override
            public Object select(Map<String, List<TransactionEvent>> match) throws Exception {
                List<TransactionEvent> start = match.get("start");
                return start;
            }
        });

        //4.sink
        result.print();

        //5.execute
        env.execute("execute cep");
    }
}