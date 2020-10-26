package cn.itcast.cep;

import cn.itcast.bean.OrderEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Date 2020/9/21
 * 用户下单以后，应该设置订单失效时间，用来提高用户的支付意愿
 * 如果用户下单15分钟未支付，则输出监控信息
 */
public class Demo7_OrderTimeoutDemo {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.source
        SingleOutputStreamOperator<OrderEvent> source = env.fromCollection(Arrays.asList(
                new OrderEvent("123", 1,"create", 1558430842000L),//2019-05-21 17:27:22
                new OrderEvent("456", 2,"create", 1558430843000L),//2019-05-21 17:27:23
                new OrderEvent("456", 2,"other", 1558430845000L), //2019-05-21 17:27:25
                new OrderEvent("456", 2,"pay", 1558430850000L)   //2019-05-21 17:27:30
        )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getEventTime();
            }
        });
        //3.transformation
        //-1.定义模式规则
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("begin")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        //定义业务规则
                        return value.getStatus().equals("create");
                    }
                }).followedBy("end")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.getStatus().equals("pay");
                    }
                }).within(Time.minutes(15));

        //-2.将规则应用到数据流
        PatternStream<OrderEvent> cep = CEP.pattern(source.keyBy(OrderEvent::getOrderId), pattern);
        //-3.设置侧输出流,封装超时数据
        OutputTag<OrderEvent> opt = new OutputTag<>("opt", TypeInformation.of(OrderEvent.class));

        //-4.获取符合规则的数据
        SingleOutputStreamOperator<Object> result = cep.select(opt,
                new PatternTimeoutFunction<OrderEvent, OrderEvent>() {//处理超时数据
                    @Override
                    public OrderEvent timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        OrderEvent timeoutData = pattern.get("begin").get(0);
                        return timeoutData;
                    }
                }, new PatternSelectFunction<OrderEvent, Object>() { //处理正常输入数据得
                    @Override
                    public Object select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        OrderEvent resultData = pattern.get("begin").get(0);
                        return resultData;
                    }
                });
        //4.sink
        result.getSideOutput(opt).print("超时数据：");
        result.print("正常数据：");

        //5.execute
        env.execute();
    }
}
