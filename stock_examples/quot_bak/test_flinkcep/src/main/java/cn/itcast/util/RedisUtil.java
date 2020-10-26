package cn.itcast.util;

import redis.clients.jedis.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Redis工具类
 */
public abstract class RedisUtil {
    /**
     * 获取集群版redis客户端JedisCluster
     */
    public static JedisCluster getJedisCluster() {
        String host = "node01:7001,node01:7002,node01:7003";//集群地址
        String maxTotal = "10"; //最大连接数
        String minIdle = "2";//最小空闲连接数
        String maxIdle = "5";//最大空闲连接数
        //获取set集合
        //node01:7001,node01:7002,node01:7003
        Set<HostAndPort> hostAndPortSet = new HashSet<>();
        String[] split = host.split(",");
        for (String str : split) {
            String[] arr = str.split(":");
            hostAndPortSet.add(new HostAndPort(arr[0], Integer.valueOf(arr[1])));
        }

        //设置连接池
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(Integer.valueOf(maxIdle));
        jedisPoolConfig.setMinIdle(Integer.valueOf(minIdle));
        jedisPoolConfig.setMaxTotal(Integer.valueOf(maxTotal));

        JedisCluster jedisCluster = new JedisCluster(hostAndPortSet, jedisPoolConfig);
        return jedisCluster;
    }

    /**
     * 获取单机版redis客户端Jedis
     */
    public static Jedis getJedis(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);//设置最大连接
        jedisPoolConfig.setMaxIdle(10);//设置最大空闲连接数
        jedisPoolConfig.setMinIdle(5);//设置最小空闲连接数
        jedisPoolConfig.setMaxWaitMillis(2000);//最大等待时间
        jedisPoolConfig.setTestOnCreate(true);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "127.0.0.1", 6379);
        return jedisPool.getResource();
    }

    public static void main(String[] args) throws IOException {
        //测试单机版
        Jedis jedis = getJedis();
        jedis.set("ck", "cv");
        String cv = jedis.get("ck");
        System.out.println(cv);
        jedis.hset("product","apple","10");
        jedis.hset("product","rice","6");
        jedis.hset("product","flour","6");
        jedis.hset("product","banana","8");
        jedis.hset("product","mask","5");
        jedis.close();

        //测试集群版
        /*JedisCluster jedisCluster = getJedisCluster();
        jedisCluster.set("ck", "cv");
        String cv = jedisCluster.get("ck");
        System.out.println(cv);
        jedisCluster.hset("quot", "zf", "-1");//振幅
        jedisCluster.hset("quot", "upDown1", "-1"); //涨跌幅-跌幅
        jedisCluster.hset("quot", "upDown2", "100");//涨跌幅-涨幅
        jedisCluster.hset("quot", "hsl", "-1");//换手率
        jedisCluster.close();*/
    }
}
