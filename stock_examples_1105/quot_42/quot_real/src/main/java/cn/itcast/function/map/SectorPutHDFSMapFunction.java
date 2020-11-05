package cn.itcast.function.map;

import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import scala.collection.mutable.StringBuilder;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Author itcast
 * Date 2020/10/27 9:33
 * Desc
 * 将SectorBean转为String(需要使用分隔符将字段拼接)
 */
public class SectorPutHDFSMapFunction implements MapFunction<SectorBean,String> {
    @Override
    public String map(SectorBean value) throws Exception {
        //1.定义字符串字段分隔符
        String seperator = QuotConfig.HDFS_SEPERATOR; //就是|
        //2.日期转换和截取
        String tradeDate = DateUtil.longTimestamp2String(value.getEventTime(), DateFormatConstant.format_yyyy_mm_dd);

        //3.新建StringBuilder对象
        StringBuilder builder = new StringBuilder();
        //4.字段拼接
        builder.append(new Timestamp(new Date().getTime())).append(seperator)
                .append(tradeDate).append(seperator)
                .append(value.getSectorCode()).append(seperator)
                .append(value.getSectorName()).append(seperator)
                .append(value.getPreClosePrice()).append(seperator)
                .append(value.getOpenPrice()).append(seperator)
                .append(value.getHighPrice()).append(seperator)
                .append(value.getLowPrice()).append(seperator)
                .append(value.getClosePrice()).append(seperator)
                .append(value.getTradeVol()).append(seperator)
                .append(value.getTradeAmt()).append(seperator)
                .append(value.getTradeVolDay()).append(seperator)
                .append(value.getTradeAmtDay());
        //5.返回拼接好的字符串
        return builder.toString();
    }
}
