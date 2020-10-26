package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.WarnBaseBean;
import cn.itcast.standard.ProcessDataInterface;
import cn.itcast.mail.MailSend;
import cn.itcast.util.RedisUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * 涨跌幅
 */
public class WarnUpdownTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据转换map
         * 2.封装样例类数据
         * 3.加载redis涨跌幅数据
         * 4.定义模式
         * 5.获取匹配模式流数据
         * 6.查询数据
         * 7.发送告警邮件
         */
        //1.数据转换map
        SingleOutputStreamOperator<WarnBaseBean> mapData = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean value) throws Exception {
                //2.封装样例类数据
                WarnBaseBean warnBaseBean = new WarnBaseBean(
                        value.getSecCode(),
                        value.getPreClosePx(),
                        value.getMaxPrice(),
                        value.getMinPrice(),
                        value.getTradePrice(),
                        value.getEventTime()
                );
                return warnBaseBean;
            }
        });

        //3.加载redis涨跌幅数据
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal upDown1 = new BigDecimal(jedisCluster.hget("quot", "upDown1"));//-1
        BigDecimal upDown2 = new BigDecimal(jedisCluster.hget("quot", "upDown2"));//50

        //4.定义模式
        Pattern<WarnBaseBean, WarnBaseBean> pattern = Pattern.<WarnBaseBean>begin("begin")
                .where(new SimpleCondition<WarnBaseBean>() {
                    @Override
                    public boolean filter(WarnBaseBean value) throws Exception {
                        //(期末收盘点位-期初前收盘点位)/期初前收盘点位*100%
                        BigDecimal up =
                                (
                                 value.getClosePrice().subtract(value.getPreClosePrice())
                                )
                                 .divide(value.getPreClosePrice(), 2, RoundingMode.HALF_UP);
                        if (up.compareTo(upDown1) == -1 || up.compareTo(upDown2) == 1) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });
        //5.将规则应用到数据流
        PatternStream<WarnBaseBean> cep = CEP.pattern(mapData.keyBy(WarnBaseBean::getSecCode), pattern);
        //8.获取符合规则的数据
        cep.select(new PatternSelectFunction<WarnBaseBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnBaseBean>> pattern) throws Exception {
                List<WarnBaseBean> list = pattern.get("begin");
                if (list.size() > 0) {
                    MailSend.send("个股涨跌幅预警：" + list.toString());//邮件发送
                }
                return list;
            }
        }).print("个股涨跌幅预警：");
    }
}
