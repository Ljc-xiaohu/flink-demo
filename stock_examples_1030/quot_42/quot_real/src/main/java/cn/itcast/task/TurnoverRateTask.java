package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.TurnoverRateBean;
import cn.itcast.mail.MailSend;
import cn.itcast.standard.ProcessDataInterface;
import cn.itcast.util.DBUtil;
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
 * Date 2020/10/30 15:39
 * Desc 个股行情监控预警:换手率核心业务类
 * 换手率 = 某一段时期内的成交量/发行总股数×100%
 * （在中国：成交量/流通股本×100%）
 * FlinkCEP中自定义的规则/模式为:60s内2次 换手率  超出阈值就发送邮件告警
 * 那么阈值应该要支持动态更新,所以可以存在Redis中!
 * 注意:个股流通股本在哪?--mysql表中
 * 也就是说:后续要查mysql和redis
 */
public class TurnoverRateTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //TODO 1.数据转换CleanBean-->TurnoverRateBean
        SingleOutputStreamOperator<TurnoverRateBean> flagDS = waterData.map(new RichMapFunction<CleanBean, TurnoverRateBean>() {
            //-1.1定义变量后续使用
            JedisCluster jedisCluster = null;
            //Map<个股代码, Map<字段名称, 字段值>>
            Map<String, Map<String, Object>> infoMap = null;

            //-1.2初识变量
            @Override
            public void open(Configuration parameters) throws Exception {
                jedisCluster = RedisUtil.getJedisCluster();
                String sql = "select sec_code,nego_cap from bdp_sector_stock";
                infoMap = DBUtil.query("sec_code", sql);
                //注意:实际中如果msyql中数据也经常变化需要在map方法中查询
                //或者每天查询mysql中的个股代码和个股流通股本然后放Redis中,那么就可以统一从Redis中查询个股流通股和换手率阈值!
            }

            //-1.3数据转换并判断是否超出阈值并设置flag
            @Override
            public TurnoverRateBean map(CleanBean cleanBean) throws Exception {
                TurnoverRateBean turnoverRateBean = new TurnoverRateBean();
                turnoverRateBean.setFlag(false);
                Map<String, Object> map = this.infoMap.get(cleanBean.getSecCode());
                if (map != null) {
                    //-1.4取出个股流通股本
                    BigDecimal nego_cap = new BigDecimal(map.get("nego_cap").toString());
                    turnoverRateBean.setNegoCap(nego_cap);
                    //-1.5换手率 = 某一段时期内的成交量/发行总股数×100%
                    //在中国：成交量/流通股本×100%
                    BigDecimal turnoverRate = new BigDecimal(cleanBean.getTradeVolumn()).divide(nego_cap, 2, RoundingMode.HALF_UP);
                    //-1.6取出阈值
                    //  jedisCluster.hset("quot", "hsl", "-1");//换手率
                    BigDecimal threshold = new BigDecimal(jedisCluster.hget("quot", "hsl"));
                    //-1.7设置falg
                    if (turnoverRate.compareTo(threshold) == 1) {//超出阈值
                        turnoverRateBean.setFlag(true);
                    }
                }
                //-1.8返回封装结果
                turnoverRateBean.setSecCode(cleanBean.getSecCode());
                turnoverRateBean.setSecName(cleanBean.getSecName());
                turnoverRateBean.setTradePrice(cleanBean.getTradePrice());
                turnoverRateBean.setTradeVol(cleanBean.getTradeVolumn());
                return turnoverRateBean;
            }
        });
        //TODO 2.定义模式:60s内2次 换手率  超出阈值就发送邮件告警
        Pattern<TurnoverRateBean, TurnoverRateBean> pattern = Pattern.<TurnoverRateBean>begin("begin")
                .where(new SimpleCondition<TurnoverRateBean>() {
                    @Override
                    public boolean filter(TurnoverRateBean turnoverRateBean) throws Exception {
                        return turnoverRateBean.getFlag();
                    }
                }).followedBy("next")
                .where(new SimpleCondition<TurnoverRateBean>() {
                    @Override
                    public boolean filter(TurnoverRateBean turnoverRateBean) throws Exception {
                        return turnoverRateBean.getFlag();
                    }
                }).within(Time.seconds(60));

        //TODO 3.将规则应用到数据流
        PatternStream<TurnoverRateBean> patternDS = CEP.pattern(flagDS.keyBy(TurnoverRateBean::getSecCode), pattern);
        //TODO 4.获取符合规则的数据并发送邮件
        SingleOutputStreamOperator<Object> result = patternDS.select(new PatternSelectFunction<TurnoverRateBean, Object>() {
            @Override
            public Object select(Map<String, List<TurnoverRateBean>> map) throws Exception {
                List<TurnoverRateBean> list = map.get("next");
                if (list.size() > 0) {
                    MailSend.send(list.toString());//发送邮件
                }
                return list;
            }
        });
        //TODO 5.数据打印
        result.print("个股行情实时预警换手率:");
    }
}
