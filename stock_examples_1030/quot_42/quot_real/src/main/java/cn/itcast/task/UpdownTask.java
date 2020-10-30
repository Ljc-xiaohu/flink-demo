package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.WarnBaseBean;
import cn.itcast.mail.MailSend;
import cn.itcast.standard.ProcessDataInterface;
import cn.itcast.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/10/30 14:52
 * Desc 个股行情涨跌幅预警核心业务类
 * 涨跌幅：(期末收盘点位-期初前收盘点位)/期初前收盘点位*100%
 * (今天的最新价 - 上一个交易日的收盘价)/ 上一个交易日的收盘价
 * FlinkCEP中自定义的规则/模式为:60s内2次 涨跌幅 超出阈值就发送邮件告警
 * 那么阈值应该要支持动态更新,所以可以存在Redis中!
 */
public class UpdownTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //TODO 1.数据转换waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
        SingleOutputStreamOperator<WarnBaseBean> flagDS = waterData.map(new RichMapFunction<CleanBean, WarnBaseBean>() {
            private JedisCluster jedisCluster = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedisCluster = RedisUtil.getJedisCluster();
            }

            @Override
            public WarnBaseBean map(CleanBean cleanBean) throws Exception {
                //转换cleanBean-->WarnBaseBean
                WarnBaseBean warnBaseBean = new WarnBaseBean();
                warnBaseBean.setSecCode(cleanBean.getSecCode());
                warnBaseBean.setPreClosePrice(cleanBean.getPreClosePx());
                warnBaseBean.setHighPrice(cleanBean.getMaxPrice());
                warnBaseBean.setLowPrice(cleanBean.getMinPrice());
                warnBaseBean.setClosePrice(cleanBean.getTradePrice());
                warnBaseBean.setFlag(false);
                warnBaseBean.setEventTime(cleanBean.getEventTime());
                //获取阈值
                BigDecimal downThreshold = new BigDecimal(jedisCluster.hget("quot", "upDown1"));//跌幅阈值
                BigDecimal upThreshold = new BigDecimal(jedisCluster.hget("quot", "upDown2"));//涨幅阈值
                //计算涨跌幅：
                // (期末收盘点位-期初前收盘点位)/期初前收盘点位*100%
                // (今天的最新价 - 上一个交易日的收盘价)/ 上一个交易日的收盘价
                BigDecimal updown = cleanBean.getTradePrice().subtract(cleanBean.getPreClosePx()).divide(cleanBean.getPreClosePx(), 2, RoundingMode.HALF_UP);
                if (updown.compareTo(downThreshold) == -1 || updown.compareTo(upThreshold) == 1) {//超出阈值
                    //根据涨跌幅和阈值比较结果设置flag为true表示超出阈值
                    warnBaseBean.setFlag(true);
                }
                //返回转换结果
                return warnBaseBean;
            }
        });
        //TODO 2.定义模式:60s内2次 涨跌幅 超出阈值就发送邮件告警
        Pattern<WarnBaseBean, WarnBaseBean> pattern = Pattern.<WarnBaseBean>begin("begin")
                .where(new SimpleCondition<WarnBaseBean>() {
                    @Override
                    public boolean filter(WarnBaseBean warnBaseBean) throws Exception {
                        return warnBaseBean.getFlag();
                    }
                })
                .followedBy("next")
                .where(new SimpleCondition<WarnBaseBean>() {
                    @Override
                    public boolean filter(WarnBaseBean warnBaseBean) throws Exception {
                        return warnBaseBean.getFlag();
                    }
                }).within(Time.seconds(60));

        //TODO 3.将规则应用到数据流
        PatternStream<WarnBaseBean> patternDS = CEP.pattern(flagDS.keyBy(WarnBaseBean::getSecCode), pattern);

        //TODO 4.获取符合规则的数据并发送告警邮件MailSend.send("个股涨跌幅预警：" + list.toString());
        SingleOutputStreamOperator<Object> result = patternDS.select(new PatternSelectFunction<WarnBaseBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnBaseBean>> map) throws Exception {
                List<WarnBaseBean> list = map.get("next");
                if (list.size() > 0) {
                    MailSend.send(list.toString());//发送邮件
                }
                return list;
            }
        });

        //TODO 5.输出结果
        result.print("个股行情涨跌幅预警:");
    }
}
