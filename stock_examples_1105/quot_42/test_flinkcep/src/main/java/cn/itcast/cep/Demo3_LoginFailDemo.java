package cn.itcast.cep;

import cn.itcast.bean.LoginUser;
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
 * 需求：
 * 识别2秒内连续登录失败用户
 * 有一个业务系统，用户要使用该业务系统必须要先登陆
 * 过滤出来在2秒内连续登陆失败的用户
 */
public class Demo3_LoginFailDemo {
    public static void main(String[] args) throws Exception {
        //开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.source
        SingleOutputStreamOperator<LoginUser> source = env.fromCollection(Arrays.asList(
                new LoginUser(1, "192.168.0.1", "fail", 1558430842000L),    //2019-05-21 17:27:22
                new LoginUser(1, "192.168.0.2", "fail", 1558430843000L),    //2019-05-21 17:27:23
                new LoginUser(1, "192.168.0.3", "fail", 1558430843000L),    //2019-05-21 17:27:23
                new LoginUser(2, "192.168.10.10", "success", 1558430845000L)//2019-05-21 17:27:25
        )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginUser>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(LoginUser element) {
                return element.getEventTime();
            }
        });

        //3.transformation
        //识别2秒内连续登录失败用户
        //开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
        //-1.定义模式规则
        Pattern<LoginUser, LoginUser> pattern = Pattern.<LoginUser>begin("start")
                .where(new SimpleCondition<LoginUser>() {
                    @Override
                    public boolean filter(LoginUser loginUser) throws Exception {
                        return loginUser.getStatus().equals("fail");
                    }
                }).next("next")
                .where(new SimpleCondition<LoginUser>() {
                    @Override
                    public boolean filter(LoginUser loginUser) throws Exception {
                        return loginUser.getStatus().equals("fail");
                    }
                }).within(Time.seconds(2));

        //-2.将规则应用到数据流等到应用了规则的流patternDS
        PatternStream<LoginUser> patternDS = CEP.pattern(source.keyBy(LoginUser::getUserId), pattern);

        //-3.获取符合规则的数据
        SingleOutputStreamOperator<Object> resultDS = patternDS.select(new PatternSelectFunction<LoginUser, Object>() {
            @Override
            public Object select(Map<String, List<LoginUser>> map) throws Exception {
                return map.get("next");
            }
        });
        //4.sink
        resultDS.print();
        //5.execute
        env.execute();
    }
}
