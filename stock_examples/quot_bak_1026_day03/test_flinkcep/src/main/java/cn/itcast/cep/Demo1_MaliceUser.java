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
 * 需求：
 * 用户如果在10s内，输入了TMD 5次，就认为用户为恶意攻击，识别出该用户
 */
public class Demo1_MaliceUser {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.source
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
        //3.transformation
        //-1.定义模式规则
        Pattern<Message, Message> pattern = Pattern.<Message>begin("begin")
                .where(new SimpleCondition<Message>() {
                    @Override
                    public boolean filter(Message value) throws Exception {
                        return value.getMsg().equals("TMD");
                    }
                })
                .times(5) //匹配次数
                //.times(3,5)
                //.oneOrMore()
                .within(Time.seconds(10));//时间长度

        //-2.将规则应用到数据流
        PatternStream<Message> cep = CEP.pattern(source.keyBy(Message::getId), pattern);
        //-3.获取符合规则的数据
        SingleOutputStreamOperator<List<Message>> result = cep.select(new PatternSelectFunction<Message, List<Message>>() {
            @Override
            public List<Message> select(Map<String, List<Message>> pattern) throws Exception {
                List<Message> begin = pattern.get("begin");
                return begin;
            }
        });

        //4.sink
        result.print();

        //5.execute
        env.execute();
    }
}
