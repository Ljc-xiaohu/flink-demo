package cn.itcast.cep;

import cn.itcast.bean.Message;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Date 2020/9/19
 */
public class Demo1_MaliceUser {
    public static void main(String[] args) throws Exception {
        /**
         * 需求：用户如果在10s内，输入了TMD 5次，就认为用户为恶意攻击，识别出该用户
         * 开发步骤：
         *  1.获取流处理执行环境
         *  2.设置事件时间
         *  3.构建数据源
         *  4.定义模式规则
         *  5.将规则应用到数据流
         *  6.获取符合规则的数据
         *  7.打印查询到的数据
         *  8.执行任务
         */
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.构建数据源
        SingleOutputStreamOperator<Message> source = env.fromCollection(Arrays.asList(
                new Message("1", "TMD", 1558430842000L),//2019-05-21 17:27:22
                new Message("1", "TMD", 1558430843000L),//2019-05-21 17:27:23
                new Message("1", "TMD", 1558430845000L),//2019-05-21 17:27:25
                new Message("1", "TMD", 1558430850000L),//2019-05-21 17:27:30
                new Message("1", "TMD", 1558430851000L),//2019-05-21 17:27:30
                new Message("2", "TMD", 1558430851000L),//2019-05-21 17:27:31
                new Message("1", "TMD", 1558430852000L)//2019-05-21 17:27:32
        )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Message>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Message element) {
                return element.getEventTime();
            }
        });

        //4.定义模式规则
        Pattern<Message, Message> pattern = Pattern.<Message>begin("begin")
                .where(new SimpleCondition<Message>() {
                    @Override
                    public boolean filter(Message value) throws Exception {
                        return value.getMsg().equals("TMD");
                    }
                })
                //.times(5) //匹配次数
                //.times(3,5)
                .oneOrMore()
                .within(Time.seconds(10));//时间长度

        //5.将规则应用到数据流
        PatternStream<Message> cep = CEP.pattern(source.keyBy(Message::getId), pattern);
        // 6.获取符合规则的数据
        SingleOutputStreamOperator<List<Message>> result = cep.select(new PatternSelectFunction<Message, List<Message>>() {
            @Override
            public List<Message> select(Map<String, List<Message>> pattern) throws Exception {
                List<Message> begin = pattern.get("begin");
                return begin;
            }
        });

        //7.打印查询到的数据
        result.print();

        //8.执行任务
        env.execute();
    }
}
