package cn.itcast.cep;

import cn.itcast.bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Date 2020/9/19
 * 需求：
 * 查询匹配用户登陆状态是fail，且失败次数大于8的数据
 */
public class Demo2_ConditionDemo {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.source
        DataStream<LoginEvent> source = env.fromCollection(Arrays.asList(
                new LoginEvent("1", "192.168.0.1", "fail", 8),
                new LoginEvent("1", "192.168.0.2", "fail", 9),
                new LoginEvent("1", "192.168.0.3", "fail", 10),
                new LoginEvent("1", "192.168.0.4", "fail", 10),
                new LoginEvent("2", "192.168.10.10", "success", -1),
                new LoginEvent("3", "192.168.10.10", "fail", 5),
                new LoginEvent("3", "192.168.10.11", "fail", 6),
                new LoginEvent("4", "192.168.10.10", "fail", 6),
                new LoginEvent("4", "192.168.10.11", "fail", 7),
                new LoginEvent("4", "192.168.10.12", "fail", 8),
                new LoginEvent("5", "192.168.10.13", "success", 8),
                new LoginEvent("5", "192.168.10.14", "success", 9),
                new LoginEvent("5", "192.168.10.15", "success", 10),
                new LoginEvent("6", "192.168.10.16", "fail", 6),
                new LoginEvent("6", "192.168.10.17", "fail", 8),
                new LoginEvent("7", "192.168.10.18", "fail", 5),
                new LoginEvent("6", "192.168.10.19", "fail", 10),
                new LoginEvent("6", "192.168.10.18", "fail", 9)
        ));
        //3.transformation
        //-1.定义模式设置匹配模式连续where，先匹配状态（多次），再匹配数量
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    //迭代条件：
                    //该条件基于先前接收事件的属性或其子集的统计信息来进行遍历取值
                    //能够对模式之前所有接收的事件进行处理
                    //调用.where((value,ctx)=>(...)),可以调用ctx.getEventsForPattern("name")
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.getStatus().equals("fail");
                    }
                })
                /*.where(new SimpleCondition<LoginEvent>() {
                //简单条件：
                //继承IterativeCondition，只需要判断事件属性是否符合相应条件即可。
                //Simple Conditions ：其主要是根据事件中的字段信息进行判断，决定是否接受该条件
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return false;
                    }
                })*/
                //.times(2)
                //.where(new SimpleCondition<LoginEvent>() {
                //.or(new SimpleCondition<LoginEvent>() {
                //.oneOrMore()
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getCount() == 8;
                    }
                });

        //-2.将规则应用到数据流
        PatternStream<LoginEvent> cep = CEP.pattern(source.keyBy(LoginEvent::getId), pattern);
        //-3.获取符合规则的数据
        cep.select(new PatternSelectFunction<LoginEvent, Object>() {
            @Override
            public Object select(Map<String, List<LoginEvent>> pattern) throws Exception {
                List<LoginEvent> begin = pattern.get("begin");
                return begin;
            }

            //4.数据打印
        }).print();

        //5.触发执行
        env.execute();
    }
}
