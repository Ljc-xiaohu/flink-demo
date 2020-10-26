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
 * @Date 2020/9/19
 */
public class Demo4_LoginFailDemo {

    public static void main(String[] args) throws Exception {

        /**
         * 过滤出来在2秒内连续登陆失败的用户
         *开发步骤：
         * 1.获取流处理执行环境
         * 2.设置并行度,设置事件时间
         * 3.加载数据源,提取事件时间
         * 4.定义匹配模式，设置时间长度
         * 5.匹配模式（分组）
         * 6.数据处理
         * 7.打印
         * 8.触发执行
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度,设置事件时间
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.加载数据源,提取事件时间
        SingleOutputStreamOperator<LoginUser> source = env.fromCollection(Arrays.asList(
                new LoginUser(1, "192.168.0.1", "fail", 1558430842000L),        //2019-05-21 17:27:22
                new LoginUser(1, "192.168.0.2", "fail", 1558430843000L),        //2019-05-21 17:27:23
                new LoginUser(1, "192.168.0.3", "fail", 1558430844000L),        //2019-05-21 17:27:24
                new LoginUser(2, "192.168.10.10", "success", 1558430845000L)    //2019-05-21 17:27:25
        )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginUser>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(LoginUser element) {
                return element.getEventTime();
            }
        });

        //4.定义匹配模式，设置时间长度
        Pattern<LoginUser, LoginUser> pattern = Pattern.<LoginUser>begin("begin")
                .where(new SimpleCondition<LoginUser>() {
                    @Override
                    public boolean filter(LoginUser value) throws Exception {
                        return value.getStatus().equals("fail");
                    }
                }).next("next")
                .where(new SimpleCondition<LoginUser>() {
                    @Override
                    public boolean filter(LoginUser value) throws Exception {
                        return value.getStatus().equals("fail");
                    }
                }).within(Time.seconds(2));
        //5.匹配模式（分组）
        PatternStream<LoginUser> cep = CEP.pattern(source.keyBy(LoginUser::getUserId), pattern);
        cep.select(new PatternSelectFunction<LoginUser, Object>() {
            @Override
            public Object select(Map<String, List<LoginUser>> pattern) throws Exception {
                List<LoginUser> next = pattern.get("next");
                return next;
            }
        }).print();

        //触发执行
        env.execute();
    }

}
