package cn.itcast.cep;

import cn.itcast.bean.Product;
import cn.itcast.util.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Date 2020/9/19
 */
public class Demo5_CepMarkets {

    public static void main(String[] args) throws Exception {
        /**
         * 需求：商品售价在1分钟之内有连续两次超过预定商品价格阀值就发送告警信息。
         * 1.获取流处理执行环境
         * 2.设置事件时间、并行度
         * 3.整合kafka
         * 4.数据转换
         * 5.process获取bean,设置status，并设置事件时间
         * 6.定义匹配模式，设置时间长度
         * 7.匹配模式（分组）
         * 8.查询告警数据
         */
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置事件时间、并行度
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.整合kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092");
        properties.setProperty("group.id", "market");
        FlinkKafkaConsumer011<String> cepConsumer = new FlinkKafkaConsumer011<>("cep", new SimpleStringSchema(), properties);
        cepConsumer.setStartFromEarliest();
        //加载数据源
        DataStreamSource<String> source = env.addSource(cepConsumer);

        //4.数据转换
        SingleOutputStreamOperator<Product> mapData = source.map(new MapFunction<String, Product>() {
            @Override
            public Product map(String value) throws Exception {
                //字符串转json
                JSONObject json = JSON.parseObject(value);
                //{"goodsId":100001,"goodsPrice":6,"goodsName":"apple",
                // "alias":"苹果","orderTime":1558430843000}
                Product product = new Product(
                        json.getLongValue("goodsId"),
                        json.getDoubleValue("goodsPrice"),
                        json.getString("goodsName"),
                        json.getString("alias"),
                        json.getLongValue("orderTime"),
                        false
                );
                return product;
            }
        }) //提取事件时间
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Product>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Product element) {
                return element.getOrderTime();
            }
        });


        //5.process获取bean,设置status，并设置事件时间
        //判断status的状态
        SingleOutputStreamOperator<Product> processData = mapData.process(new ProcessFunction<Product, Product>() {

            //获取reids中的阀值数据
            JedisCluster jedisCluster = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedisCluster = RedisUtil.getJedisCluster();
            }

            @Override
            public void processElement(Product value, Context ctx, Collector<Product> out) throws Exception {

                //获取阀值
                String threshold = jedisCluster.hget("product", value.getGoodsName());
                if (value.getGoodsPrice() > Double.valueOf(threshold)) {
                    value.setStatus(true);
                }
                out.collect(value);
            }
        });

        //6.定义匹配模式，设置时间长度
        Pattern<Product, Product> pattern = Pattern.<Product>begin("begin")
                .where(new SimpleCondition<Product>() {
                    @Override
                    public boolean filter(Product value) throws Exception {
                        return value.getStatus() == true;
                    }
                }).next("next")
                .where(new SimpleCondition<Product>() {
                    @Override
                    public boolean filter(Product value) throws Exception {
                        return value.getStatus() == true;
                    }
                }).within(Time.minutes(1));
        //模式作用在实时数据流上
        PatternStream<Product> cep = CEP.pattern(processData.keyBy(Product::getGoodsId), pattern);
        //8.查询告警数据
        SingleOutputStreamOperator<List<Product>> result = cep.select(new PatternSelectFunction<Product, List<Product>>() {
            @Override
            public List<Product> select(Map<String, List<Product>> pattern) throws Exception {
                List<Product> next = pattern.get("next");
                return next;
            }
        });

        //告警数据打印
        result.print();
        env.execute();
    }
}
