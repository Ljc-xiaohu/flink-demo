package cn.itcast.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;

/**
 * Redis配置类
 */
@Configuration
public class RedisConfig {
    @Value("${redis.maxtotal}")
    private int maxTotal;
    @Value("${redis.minIdle}")
    private int minIdle;
    @Value("${redis.maxIdle}")
    private int maxIdle;
    @Value("${redis.address}")
    private String address;

    @Bean
    public JedisCluster getJedisCluster(){
        HashSet<HostAndPort> set = new HashSet<>();
        String[] split = address.split(",");
        for (String str : split) {
            String[] arr = str.split(":");
            set.add(new HostAndPort(arr[0],Integer.valueOf(arr[1])));
        }

        //设置连接池
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMinIdle(minIdle);
        jedisPoolConfig.setMaxTotal(maxTotal);
        JedisCluster jedisCluster = new JedisCluster(set, jedisPoolConfig);

        return jedisCluster;
    }

}
