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
 * Author itcast
 * Date 2020/10/29 15:57
 * Desc
 * 需求
 * 识别恶意用户
 * 用户如果在10s内，输入了TMD 5次，就认为用户为恶意攻击，识别出该用户
 * 使用 Flink CEP量词模式
 * <p>
 * 开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
 */
public class Demo1_MaliceUser {
    public static void main(String[] args) throws Exception {
        //开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.source
        SingleOutputStreamOperator<Message> source = env.fromCollection(Arrays.asList(
                new Message("1", "TMD", 1558430842000L),//2019-05-21 17:27:22
                new Message("1", "TMD", 1558430843000L),//2019-05-21 17:27:23
                new Message("1", "TMD", 1558430845000L),//2019-05-21 17:27:25
                new Message("1", "TMD", 1558430850000L),//2019-05-21 17:27:30
                new Message("1", "TMD", 1558430851000L),//2019-05-21 17:27:31
                new Message("2", "TMD", 1558430851000L),//2019-05-21 17:27:31
                new Message("3", "TMD", 1558430852000L)//2019-05-21 17:27:32
        )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Message>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Message element) {
                return element.getEventTime();
            }
        });

        //3.transformation
        //识别恶意用户
        //用户如果在10s内，输入了TMD 5次，就认为用户为恶意攻击，识别出该用户
        //使用 Flink CEP量词模式
        //开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
        //-1.定义模式规则
        Pattern<Message, Message> pattern = Pattern.<Message>begin("start").where(new SimpleCondition<Message>() {
            @Override
            public boolean filter(Message message) throws Exception {
                if (message.getMsg().equals("TMD")) {
                    return true;
                }
                return false;
            }
        }).times(5)
          .within(Time.seconds(10));

        //-2.将规则应用到数据流等到应用了规则的流patternDS
        PatternStream<Message> patternDS = CEP.pattern(source.keyBy(Message::getId), pattern);

        //-3.获取符合规则的数据
        SingleOutputStreamOperator<List<Message>> resultDS = patternDS.select(new PatternSelectFunction<Message, List<Message>>() {
            @Override
            public List<Message> select(Map<String, List<Message>> map) throws Exception {
                List<Message> resultMessage = map.get("start");//取出满足start规则的数据
                return resultMessage;
            }
        });
        //4.sink
        resultDS.print("被FlinkCEP规则检测到的恶意用户:");
        //5.execute
        env.execute();
    }
}
