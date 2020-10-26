package cn.itcast.cep;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @Date 2020/9/19
 * 需求：
 * 从数据源中依次提取"c","a","b"元素
 */
public class Demo4_ConsecutiveDemo {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.source
        DataStreamSource<String> source = env.fromElements("c", "d", "a", "a", "a", "d", "a", "b");
        //3.transformation

        //-1.设置匹配模式，匹配"c","a"...,"b" 使用.consecutive()和.allowCombinations()
        Pattern<String, String> pattern = Pattern.<String>begin("begin")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("c");
                    }
                })
                .followedBy("middle")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("a");
                    }
                })
                .oneOrMore()
                //.consecutive()//连续匹配a
                /*
                1)使用consecutive()
                ([c],[a, a, a],[b])
                ([c],[a, a],[b])
                ([c],[a],[b])
                2)不使用consecutive()
                ([c],[a, a, a, a],[b])
                ([c],[a, a, a],[b])
                ([c],[a, a],[b])
                ([c],[a],[b])
                */
                .allowCombinations() //允许组合
                /*
                1)使用allowCombinations()
                ([c],[a, a, a, a],[b])
                ([c],[a, a, a],[b])
                ([c],[a, a, a],[b])
                ([c],[a, a],[b])
                ([c],[a, a, a],[b])
                ([c],[a, a],[b])
                ([c],[a, a],[b])
                ([c],[a],[b])
                */
                .followedBy("end")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("b");
                    }
                });

        //-2.将规则应用到数据流
        PatternStream<String> cep = CEP.pattern(source, pattern);
        //-3.获取符合规则的数据Tuple3.of(begin, middle, end)
        cep.select(new PatternSelectFunction<String, Object>() {
            @Override
            public Object select(Map<String, List<String>> pattern) throws Exception {
                //取出每一个模式下的匹配数据
                List<String> begin = pattern.get("begin");
                List<String> middle = pattern.get("middle");
                List<String> end = pattern.get("end");
                return Tuple3.of(begin, middle, end);
            }
            //4.sink
        }).print();

        //5.execute
        env.execute();
    }
}
