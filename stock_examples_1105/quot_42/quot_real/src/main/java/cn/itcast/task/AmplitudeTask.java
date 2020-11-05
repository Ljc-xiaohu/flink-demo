package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.WarnAmplitudeBean;
import cn.itcast.bean.WarnBaseBean;
import cn.itcast.mail.MailSend;
import cn.itcast.standard.ProcessDataWithEnvInterface;
import cn.itcast.util.RedisUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/10/30 11:49
 * Desc 个股振幅核心任务类型
 * 振幅： (max(最高点位)-min(最低点位))/期初前收盘点位*100%
 */
public class AmplitudeTask implements ProcessDataWithEnvInterface {
    @Override
    public void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env) {
        //TODO 1.数据转换waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
        SingleOutputStreamOperator<WarnBaseBean> warnDS = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean cleanBean) throws Exception {
                WarnBaseBean warnBaseBean = new WarnBaseBean();
                warnBaseBean.setSecCode(cleanBean.getSecCode());
                warnBaseBean.setPreClosePrice(cleanBean.getPreClosePx());
                warnBaseBean.setHighPrice(cleanBean.getMaxPrice());
                warnBaseBean.setLowPrice(cleanBean.getMinPrice());
                warnBaseBean.setClosePrice(cleanBean.getTradePrice());
                warnBaseBean.setFlag(false);
                warnBaseBean.setEventTime(cleanBean.getEventTime());
                return warnBaseBean;
            }
        });
        //TODO 2.初始化sql执行环境StreamTableEnvironment tbleEnv = TableEnvironment.getTableEnvironment(env);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        //TODO 3.注册表tbleEnv.registerDataStream("tbl", mapData, "secCode,preClosePrice,highPrice,lowPrice,eventTime.rowtime");
        tableEnv.registerDataStream("t_warn", warnDS, "secCode,preClosePrice,highPrice,lowPrice,flag,eventTime.rowtime");
        //TODO 4.编写并执行sql
/*
select
secCode,
preClosePrice,
max(highPrice) as highPrice,
min(lowPrice) as lowPrice,
flag
from t_warn
 group by secCode,preClosePrice,flag
 tumble(eventTime,interval '5' second ) --滚动窗口 5s
 */
        String sql = "select secCode,preClosePrice,max(highPrice) as highPrice,min(lowPrice) as lowPrice,flag from t_warn group by secCode,preClosePrice,flag,tumble(eventTime,interval '2' second )";
        Table table = tableEnv.sqlQuery(sql);

        //TODO 5.表转流tbleEnv.toAppendStream(table, WarnAmplitudeBean.class);
        DataStream<WarnAmplitudeBean> warnAmplitudeDS = tableEnv.toAppendStream(table, WarnAmplitudeBean.class);

        //TODO 6.获取redis振幅阀值并判断计算振幅是否超出阈值并设置flag
        //振幅： (max(最高点位)-min(最低点位))/期初前收盘点位*100%
        SingleOutputStreamOperator<WarnAmplitudeBean> flagDS = warnAmplitudeDS.map(new RichMapFunction<WarnAmplitudeBean, WarnAmplitudeBean>() {
            JedisCluster jedisCluster = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                jedisCluster = RedisUtil.getJedisCluster();
            }

            @Override
            public WarnAmplitudeBean map(WarnAmplitudeBean warnAmplitudeBean) throws Exception {
                //获取redis振幅阀值
                BigDecimal Threshold = new BigDecimal(jedisCluster.hget("quot", "zf"));
                //振幅： (max(最高点位)-min(最低点位))/期初前收盘点位*100%
                BigDecimal zf = warnAmplitudeBean.getHighPrice().subtract(warnAmplitudeBean.getLowPrice()).divide(warnAmplitudeBean.getPreClosePrice(), 2, RoundingMode.HALF_UP);
                if (zf.compareTo(Threshold) == 1) {
                    warnAmplitudeBean.setFlag(true);//表示超过阈值
                } else {
                    warnAmplitudeBean.setFlag(false);//表示未超过阈值
                }
                return warnAmplitudeBean;
            }
        });

        //TODO 7.定义模式/规则:如果60s内 振幅 > 阈值 2次就发送邮件告警!
        Pattern<WarnAmplitudeBean, WarnAmplitudeBean> pattern = Pattern
                .<WarnAmplitudeBean>begin("begin").where(new SimpleCondition<WarnAmplitudeBean>() {
                    @Override
                    public boolean filter(WarnAmplitudeBean warnAmplitudeBean) throws Exception {
                        return warnAmplitudeBean.getFlag();
                    }
                })
                .followedBy("next")
                .where(new SimpleCondition<WarnAmplitudeBean>() {
                    @Override
                    public boolean filter(WarnAmplitudeBean warnAmplitudeBean) throws Exception {
                        return warnAmplitudeBean.getFlag();
                    }
                })
                .within(Time.seconds(60));

        //TODO 8.将规则应用到数据流CEP.pattern(amplitudeData.keyBy(WarnAmplitudeBean::getSecCode), pattern);
        PatternStream<WarnAmplitudeBean> patternDS = CEP.pattern(flagDS.keyBy(WarnAmplitudeBean::getSecCode), pattern);
        //TODO 9.获取结果并发送告警邮件MailSend.send("个股振幅预警:" + list.toString());
        SingleOutputStreamOperator<Object> result = patternDS.select(new PatternSelectFunction<WarnAmplitudeBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnAmplitudeBean>> map) throws Exception {
                List<WarnAmplitudeBean> list = map.get("next");
                if(list.size() > 0){
                    MailSend.send(list.toString());//发邮件告警
                }
                return list;
            }
        });
        //TODO 10.输出结果
        result.print("个股振幅预警信息:");
    }
}
