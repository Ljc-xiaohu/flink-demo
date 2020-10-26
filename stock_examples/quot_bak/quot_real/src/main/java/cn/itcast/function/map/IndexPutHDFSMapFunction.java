package cn.itcast.function.map;

import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.Timestamp;

/**
 */
public class IndexPutHDFSMapFunction extends RichMapFunction<IndexBean,String> {
    // 1.定义字符串字段分隔符
    String sp = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(IndexBean value) throws Exception {
        /**
         * 开发步骤:
         * 1.定义字符串字段分隔符
         * 2.日期转换和截取：date类型
         * 3.新建字符串缓存对象
         * 4.封装字符串数据
         *
         * 字符串拼装字段顺序：
         * Timestamp|date|indexCode|indexName|preClosePrice|openPirce|highPrice|
         * lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
         */
        //2.日期转换和截取：date类型
        String tradeDate = DateUtil.longTimestamp2String(value.getEventTime(), DateFormatConstant.format_yyyy_mm_dd);
        //3.新建字符串缓存对象
        StringBuilder builder = new StringBuilder();
        builder.append(new Timestamp(value.getEventTime())).append(sp)
                .append(tradeDate).append(sp)
                .append(value.getIndexCode()).append(sp)
                .append(value.getIndexName()).append(sp)
                .append(value.getPreClosePrice()).append(sp)
                .append(value.getOpenPrice()).append(sp)
                .append(value.getHighPrice()).append(sp)
                .append(value.getLowPrice()).append(sp)
                .append(value.getClosePrice()).append(sp)
                .append(value.getTradeVol()).append(sp)
                .append(value.getTradeAmt()).append(sp)
                .append(value.getTradeVolDay()).append(sp)
                .append(value.getTradeAmtDay()).append(sp)
                .append(value.getSource());

        return builder.toString();
    }
}
