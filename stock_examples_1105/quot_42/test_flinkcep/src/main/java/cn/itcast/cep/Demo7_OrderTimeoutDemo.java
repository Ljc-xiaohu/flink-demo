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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date 2020/9/21
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
        //如果用户下单15分钟未支付，则输出监控信息
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
        //-3.设置侧输出流,用来封装超时数据
        OutputTag<OrderEvent> timeoutTag = new OutputTag<>("opt", TypeInformation.of(OrderEvent.class));

        //-4.获取超时数据和符合规则的数据
        SingleOutputStreamOperator<Object> result = cep.select(timeoutTag,//接收超时数据
                new PatternTimeoutFunction<OrderEvent, OrderEvent>() {//处理超时数据
                    @Override
                    //Map<规则名称, 符合规则的数据> map
                    public OrderEvent timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        return map.get("begin").get(0);
                    }
                }, new PatternSelectFunction<OrderEvent, Object>() {//处理正常数据
                    @Override
                    public Object select(Map<String, List<OrderEvent>> map) throws Exception {
                        List<OrderEvent> begin = map.get("begin");
                        List<OrderEvent> end = map.get("end");
                        begin.addAll(end);//把end集合中的所有元素添加到begin集合中,最后返回begin集合即可
                        return begin;
                    }
                });
        //4.sink
        result.print("正常数据:");
        DataStream<OrderEvent> timeoutDS = result.getSideOutput(timeoutTag);
        timeoutDS.print("超时数据");

        //5.execute
        env.execute();
    }
}
