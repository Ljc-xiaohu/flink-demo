package cn.itcast.function.map;

import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import scala.collection.mutable.StringBuilder;

import java.sql.Timestamp;

/**
 * Author itcast
 * Date 2020/10/27 9:33
 * Desc
 * 将StockBean转为String(需要使用分隔符将字段拼接)
 */
public class StockPutHDFSMapFunction implements MapFunction<StockBean,String> {
    @Override
    public String map(StockBean value) throws Exception {
        //1.定义字符串字段分隔符
        String seperator = QuotConfig.HDFS_SEPERATOR; //就是|
        //2.日期转换和截取
        String tradeDate = DateUtil.longTimestamp2String(value.getEventTime(), DateFormatConstant.format_yyyy_mm_dd);

        //3.新建StringBuilder对象
        StringBuilder builder = new StringBuilder();
        //4.字段拼接
        //字符串拼装字段顺序：
        //Timestamp|date|secCode|secName|preClosePrice|openPirce|highPrice|
        //lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
        builder.append(new Timestamp(value.getEventTime())).append(seperator)
                .append(tradeDate).append(seperator)
                .append(value.getSecCode()).append(seperator)
                .append(value.getSecName()).append(seperator)
                .append(value.getPreClosePrice()).append(seperator)
                .append(value.getOpenPrice()).append(seperator)
                .append(value.getHighPrice()).append(seperator)
                .append(value.getLowPrice()).append(seperator)
                .append(value.getClosePrice()).append(seperator)
                .append(value.getTradeVol()).append(seperator)
                .append(value.getTradeAmt()).append(seperator)
                .append(value.getTradeVolDay()).append(seperator)
                .append(value.getTradeAmtDay()).append(seperator)
                .append(value.getSource());
        //5.返回拼接好的字符串
        return builder.toString();
    }
}
