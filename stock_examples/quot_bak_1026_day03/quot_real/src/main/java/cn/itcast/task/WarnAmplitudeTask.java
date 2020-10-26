package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.WarnAmplitudeBean;
import cn.itcast.bean.WarnBaseBean;
import cn.itcast.standard.ProcessDataCepInterface;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * 振幅
 */
public class WarnAmplitudeTask implements ProcessDataCepInterface {
    @Override
    public void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env) {
        /**
         * 总体开发步骤：
         * 1.数据转换
         * 2.初始化表执行环境
         * 3.注册表（流）
         * 4.sql执行
         * 5.表转流
         * 6.模式匹配
         * 7.查询数据
         * 8.发送告警邮件
         */
        //1.数据转换
        SingleOutputStreamOperator<WarnBaseBean> mapData = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean value) throws Exception {
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

        //2.初始化表执行环境
        StreamTableEnvironment tbleEnv = TableEnvironment.getTableEnvironment(env);
        //3.注册表/无界流
        //secCode、preClosePrice、highPrice、lowPrice、closePrice、eventTime
        //rowtime:指定事件时间
        tbleEnv.registerDataStream("tbl", mapData, "secCode,preClosePrice,highPrice,lowPrice,eventTime.rowtime");
        //4.sql执行
        /*
SELECT
	secCode,
	preClosePrice,
	max(highPrice) AS highPrice,
	min(lowPrice) AS lowPrice
FROM
	tbl
GROUP BY
	secCode,
	preClosePrice,
	tumble (eventTime,INTERVAL '2' SECOND)
         */
        String sql = "select secCode,preClosePrice,max(highPrice) as highPrice,min(lowPrice) as lowPrice from " +
                "tbl group by secCode,preClosePrice,tumble(eventTime,interval '2' second )";
        Table table = tbleEnv.sqlQuery(sql);

        //5.表转流
        DataStream<WarnAmplitudeBean> amplitudeData = tbleEnv.toAppendStream(table, WarnAmplitudeBean.class);

        //获取redis振幅阀值
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        String amplThreshold = jedisCluster.hget("quot", "zf");

        //6.定义模式
        Pattern<WarnAmplitudeBean, WarnAmplitudeBean> pattern = Pattern.<WarnAmplitudeBean>begin("begin")
                .where(new SimpleCondition<WarnAmplitudeBean>() {
                    @Override
                    public boolean filter(WarnAmplitudeBean value) throws Exception {
                        //振幅： (max(最高点位)-min(最低点位))/期初前收盘点位*100%
                        BigDecimal amplVal =
                                (
                                 value.getHighPrice().subtract(value.getLowPrice())
                                )
                                .divide(value.getPreClosePrice(), 2, RoundingMode.HALF_UP);
                        if (amplVal.compareTo(new BigDecimal(amplThreshold)) == 1) {
                            return true;
                        }
                        return false;
                    }
                });
        //7.将规则应用到数据流
        PatternStream<WarnAmplitudeBean> cep = CEP.pattern(amplitudeData.keyBy(WarnAmplitudeBean::getSecCode), pattern);
        //8.获取符合规则的数据
        cep.select(new PatternSelectFunction<WarnAmplitudeBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnAmplitudeBean>> pattern) throws Exception {
                List<WarnAmplitudeBean> list = pattern.get("begin");
                if (list.size() > 0) {
                    //邮件告警
                    MailSend.send("个股振幅预警:" + list.toString());
                }
                return list;
            }
        }).print("个股振幅预警：");
    }
}
