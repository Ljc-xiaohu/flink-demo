package cn.itcast.cep;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/10/14 22:39
 * Desc
 * 模拟电商网站用户搜索的数据来作为数据的输入源，然后查找其中重复搜索某一个商品的人，并且发送一条告警消息。
 */
public class Demo5_Search {
    public static void main(String[] args) throws Exception{
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.source
        DataStreamSource source = env.fromElements(
                Tuple3.of("Marry", "外套", 1L),
                Tuple3.of("Marry", "帽子",1L),
                Tuple3.of("Marry", "帽子",2L),
                Tuple3.of("Marry", "帽子",3L),
                Tuple3.of("Ming", "衣服",1L),
                Tuple3.of("Marry", "鞋子",1L),
                Tuple3.of("Marry", "鞋子",2L),
                Tuple3.of("LiLei", "帽子",1L),
                Tuple3.of("LiLei", "帽子",2L),
                Tuple3.of("LiLei", "帽子",3L)
        );

        //3.transformation
        //-1.定义模式规则,寻找连续搜索帽子的用户
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
                .<Tuple3<String, String, Long>>begin("start")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                }) //.timesOrMore(3);
                .next("middle")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                });

        //-2.将规则应用到数据流
        PatternStream patternStream = CEP.pattern(source.keyBy(0), pattern);
        //-3.获取符合规则的数据
        SingleOutputStreamOperator matchStream = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
            @Override
            public String select(Map<String, List<Tuple3<String, String, Long>>> pattern) throws Exception {
                List<Tuple3<String, String, Long>> middle = pattern.get("middle");
                return middle.get(0).f0 + ":" + middle.get(0).f2 + ":" + "连续搜索两次帽子!";
            }
        });
        //4.sink
        matchStream.printToErr();
        //5.execute
        env.execute("execute cep");
    }
}
