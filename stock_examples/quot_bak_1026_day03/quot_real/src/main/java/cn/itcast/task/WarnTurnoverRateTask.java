package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.TurnoverRateBean;
import cn.itcast.standard.ProcessDataInterface;
import cn.itcast.mail.MailSend;
import cn.itcast.util.DBUtil;
import cn.itcast.util.RedisUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * 换手率
 */
public class WarnTurnoverRateTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.创建bean对象
         * 2.数据转换process
         *   (1)加载mysql流通股本数据
         *   (2)封装样例类数据
         * 3.加载redis换手率数据
         * 4.模式匹配
         * 5.查询数据
         * 6.发送告警邮件
         * 7.数据打印
         */

        //2.数据转换process
        SingleOutputStreamOperator<TurnoverRateBean> processData = waterData.process(new ProcessFunction<CleanBean, TurnoverRateBean>() {
            Map<String, Map<String, Object>> map = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                //(1)加载mysql流通股本数据
                String sql = "select sec_code,nego_cap from bdp_sector_stock";
                map = DBUtil.query("sec_code", sql);
            }

            @Override
            public void processElement(CleanBean value, Context ctx, Collector<TurnoverRateBean> out) throws Exception {
                //(2)封装样例类数据
                Map<String, Object> mapNegoCap = map.get(value.getSecCode());
                if (mapNegoCap != null) {
                    BigDecimal negoCap = new BigDecimal(mapNegoCap.get("nego_cap").toString());
                    ////secCode、secName、tradePrice、tradeVol、negoCap
                    out.collect(new TurnoverRateBean(
                            value.getSecCode(),
                            value.getSecName(),
                            value.getTradePrice(),
                            value.getTradeVolumn(),
                            negoCap
                    ));
                }
            }
        });

        //3.加载redis换手率阀值数据
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal turnoverRate = new BigDecimal(jedisCluster.hget("quot", "hsl"));

        //6.定义模式
        Pattern<TurnoverRateBean, TurnoverRateBean> pattern = Pattern.<TurnoverRateBean>begin("begin")
                .where(new SimpleCondition<TurnoverRateBean>() {
                    @Override
                    public boolean filter(TurnoverRateBean value) throws Exception {
                        //计算换手率=成交量/流通股本×100%
                        BigDecimal rate = new BigDecimal(value.getTradeVol()).divide(value.getNegoCap(), 2, RoundingMode.HALF_UP);
                        if (rate.compareTo(turnoverRate) == 1) {
                            return true;
                        }
                        return false;
                    }
                });
        //7.将规则应用到数据流
        PatternStream<TurnoverRateBean> cep = CEP.pattern(processData.keyBy(TurnoverRateBean::getSecCode), pattern);
        //8.获取符合规则的数据
        cep.select(new PatternSelectFunction<TurnoverRateBean, Object>() {
            @Override
            public Object select(Map<String, List<TurnoverRateBean>> pattern) throws Exception {
                List<TurnoverRateBean> list = pattern.get("begin");
                if (!list.isEmpty()) {
                    //发送邮件
                    MailSend.send("个股换手率预警：" + list.toString());
                }
                return list;
            }
        }).print("个股换手率预警：");
    }
}
