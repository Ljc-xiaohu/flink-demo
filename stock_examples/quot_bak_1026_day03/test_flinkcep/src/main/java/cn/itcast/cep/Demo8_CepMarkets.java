package cn.itcast.cep;

import cn.itcast.bean.Product;
import cn.itcast.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Date 2020/9/19
 * 监控在1分钟之内有连续两次超过预定商品价格阀值的商品
 */
public class Demo8_CepMarkets {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.source
        SingleOutputStreamOperator<Product> source = env.fromCollection(Arrays.asList(
                new Product(100001L, 6.0, "apple", "苹果", 1558430843000L, false),
                new Product(100007L, 0.5, "mask", "口罩", 1558430844000L, false),
                new Product(100002L, 2.0, "rice", "大米", 1558430845000L, false),
                new Product(100003L, 2.0, "flour", "面粉", 1558430846000L, false),
                new Product(100004L, 12.0, "rice", "大米", 1558430847000L, false),
                new Product(100005L, 20.0, "apple", "苹果", 1558430848000L, false),
                new Product(100006L, 3.0, "banana", "香蕉", 1558430849000L, false),
                new Product(100007L, 10.0, "mask", "口罩", 1558430850000L, false),
                new Product(100001L, 16.0, "apple", "苹果", 1558430852000L, false),
                new Product(100007L, 15.0, "mask", "口罩", 1558430853000L, false),
                new Product(100002L, 12.0, "rice", "大米", 1558430854000L, false),
                new Product(100003L, 12.0, "flour", "面粉", 1558430855000L, false),
                new Product(100004L, 12.0, "rice", "大米", 1558430856000L, false),
                new Product(100005L, 20.0, "apple", "苹果", 1558430857000L, false),
                new Product(100006L, 13.0, "banana", "香蕉", 1558430858000L, false),
                new Product(100007L, 10.0, "mask", "口罩", 1558430859000L, false))
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Product>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Product element) {
                return element.getOrderTime();
            }
        });

        //3.transformation
        //-0.根据redis中的规则判断status的状态
        SingleOutputStreamOperator<Product> productDS = source.map(new RichMapFunction<Product, Product>() {
            Jedis jedis = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = RedisUtil.getJedis();
            }

            @Override
            public Product map(Product product) throws Exception {
                //获取阀值
                String threshold = jedis.hget("product", product.getGoodsName());
                if (product.getGoodsPrice() > Double.valueOf(threshold)) {
                    product.setStatus(true);
                }
                return product;
            }
        });

        //-1.定义模式规则
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
        //-2.将规则应用到数据流
        PatternStream<Product> cep = CEP.pattern(productDS.keyBy(Product::getGoodsId), pattern);
        //-3.获取符合规则的数据
        SingleOutputStreamOperator<List<Product>> result = cep.select(new PatternSelectFunction<Product, List<Product>>() {
            @Override
            public List<Product> select(Map<String, List<Product>> pattern) throws Exception {
                List<Product> next = pattern.get("next");
                return next;
            }
        });
        //4.sink
        result.print();
        //5.execute
        env.execute();
    }
}
