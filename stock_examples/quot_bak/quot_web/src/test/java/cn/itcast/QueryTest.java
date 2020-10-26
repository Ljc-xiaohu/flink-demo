package cn.itcast;//package cn.itcast;

import cn.itcast.mapper.QuotMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;

/**
 * @Date 2020/9/22
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class QueryTest {
    @Autowired
    QuotMapper quotMapper;

    /**
     * 测试：查询mysql中的表
     */
    @Test
    public void query() {
        List<Map<String, Object>> list = quotMapper.query();
        list.forEach(System.out::println);
    }


    @Autowired
    JedisCluster jedisCluster;
    @Test
    public void queryRedis(){
        String val = jedisCluster.hget("quot", "upDown1");
        System.out.println("<<<<<<<<<<<<<:"+val);
    }
}
