package cn.itcast.cep;

import cn.itcast.bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/10/29 15:57
 * Desc
 * 需求
 * 识别出登录失败一定次数的用户
 * 查询匹配用户登陆状态是fail，且失败次数大于8的数据
 * 使用FlinkCEP条件模式
 */
public class Demo2_ConditionDemo {
    public static void main(String[] args) throws Exception {
        //开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
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
        //识别出登录失败一定次数的用户
        //查询匹配用户登陆状态是fail，且失败次数大于8的数据

        //开发步骤： FlinkCEP = 实时流数据  + 规则(模式) ==>匹配结果输出
        //-1.定义模式规则
        Pattern<LoginEvent, LoginEvent> pattern1 = Pattern.<LoginEvent>begin("start1").where(new SimpleCondition<LoginEvent>() {//简单条件
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                //System.out.println("进入start1规则判断");
                if (loginEvent.getStatus().equals("fail")) {
                    //System.out.println("状态是fail，且count>8");
                    return true;
                }
                return false;
            }
        }).where(new SimpleCondition<LoginEvent>() {//简单条件
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                //System.out.println("进入start1规则判断");
                if (loginEvent.getCount() > 8) {
                    //System.out.println("状态是fail，且count>8");
                    return true;
                }
                return false;
            }
        });

        /*Pattern<LoginEvent, LoginEvent> pattern1 = Pattern.<LoginEvent>begin("start1").where(new SimpleCondition<LoginEvent>() {//简单条件
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                //System.out.println("进入start1规则判断");
                if (loginEvent.getStatus().equals("fail") && loginEvent.getCount() > 8) {
                    //System.out.println("状态是fail，且count>8");
                    return true;
                }
                return false;
            }
        });*/

        /*Pattern<LoginEvent, LoginEvent> pattern2 = Pattern.<LoginEvent>begin("start2").where(new IterativeCondition<LoginEvent>() {//迭代条件
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                //迭代条件除了有数据流参数之外,还有context
                //context.getEventsForPattern("其他的pattern名称")
                if (loginEvent.getStatus().equals("fail") && loginEvent.getCount() > 8) {
                    return true;
                }
                return false;
            }
        });*/

        //-2.将规则应用到数据流等到应用了规则的流patternDS
        PatternStream<LoginEvent> patternDS = CEP.pattern(source.keyBy(LoginEvent::getId), pattern1);

        //-3.获取符合规则的数据
        SingleOutputStreamOperator<List<LoginEvent>> resultDS = patternDS.select(new PatternSelectFunction<LoginEvent, List<LoginEvent>>() {
            @Override
            public List<LoginEvent> select(Map<String, List<LoginEvent>> map) throws Exception {
                return map.get("start1");
            }
        });
        //4.sink
        resultDS.print();
        //5.execute
        env.execute();
    }
}
