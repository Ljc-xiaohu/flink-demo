package cn.itcast.function.map;

import cn.itcast.bean.IndexBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DBUtil;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/10/29 9:49
 * Desc
 * 指数K线数据转换和计算IndexBean-->Row
 * 日K,周K,月K通用!
 */
public class IndexKlineMapFunction extends RichMapFunction<IndexBean, Row> {
    //开发步骤：
    //一.定义变量并初始化
    //开发步骤：
    //1.定义变量
    private String firstTradeDate;//周期内首个交易日日期值
    private String tradeDate;//T日,当前日期
    //Map<index_code, Map<字段名, 字段值>>
    private Map<String, Map<String, Object>> aggMap;
    private String type; //K线类型 1为日K,2为周K,3为月K
    private String firstTradeDateName; //周期内首个交易日字段名日K:trade_date,周K:week_first_txdate,月K:month_first_txdate
    //2.构造方法初始化变量(kType：K线类型,firstTxdate：周期内首个交易日字段名)
    public IndexKlineMapFunction(String type, String firstTradeDateName) {
        this.type = type;
        this.firstTradeDateName = firstTradeDateName;
    }
    //3.open方法初始化变量
    @Override
    public void open(Configuration parameters) throws Exception {
        //3.1获取交易日历表交易日数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date = CURDATE()";
        //Map<字段名, 字段值>
        Map<String, String> tradeDateMap = DBUtil.queryKv(sql);
        //3.2根据字段名获取周期内首个交易日
        firstTradeDate = tradeDateMap.get(firstTradeDateName);//根据字段名取字段值
        //3.3获取T日当天日期
        tradeDate = tradeDateMap.get("trade_date");
        //3.4.获取K线下的汇总数据aggMap:高MAX(high_price)、低MIN(low_price)、成交量SUM(trade_vol)、成交金额SUM(trade_amt)
/*
SELECT
    index_code,
    MAX(high_price) AS high_price,
    MIN(low_price) AS low_price,
    SUM(trade_vol) AS trade_vol,
    SUM(trade_amt) AS trade_amt
FROM
    bdp_quot_index_kline_day
WHERE
    trade_date BETWEEN '2020-10-22' and '2020-10-22'
GROUP BY
    index_code
 */
        String aggSQL = "SELECT\n" +
                "    index_code,\n" +
                "    MAX(high_price) AS high_price,\n" +
                "    MIN(low_price) AS low_price,\n" +
                "    SUM(trade_vol) AS trade_vol,\n" +
                "    SUM(trade_amt) AS trade_amt\n" +
                "FROM\n" +
                "    bdp_quot_index_kline_day\n" +
                "WHERE\n" +
                "    trade_date BETWEEN '" + firstTradeDate + "' and '" + tradeDate + "'\n" +
                "GROUP BY\n" +
                "    index_code";
        aggMap = DBUtil.query("index_code", aggSQL);
    }

    //二.数据转换indexBean-->Row
    @Override
    public Row map(IndexBean indexBean) throws Exception {
        //开发步骤：
        //1.获取当前指数数据:前收、收、开、高、低、成交量、成交金额
        String indexCode = indexBean.getIndexCode();
        BigDecimal preClosePrice = indexBean.getPreClosePrice();
        BigDecimal closePrice = indexBean.getClosePrice();
        BigDecimal openPrice = indexBean.getOpenPrice();
        BigDecimal highPrice = indexBean.getHighPrice();
        BigDecimal lowPrice = indexBean.getLowPrice();
        Long tradeVolDay = indexBean.getTradeVolDay();
        Long tradeAmtDay = indexBean.getTradeAmtDay();
        BigDecimal avgPrice = new BigDecimal(0);

        //2.获取T日和周期内首次交易日时间并转换成long型
        //当天20201029
        Long tradeDateTime = DateUtil.stringToLong(tradeDate, DateFormatConstant.format_yyyy_mm_dd);
        //周期内首个交易日:日K20201029,周K20201026,月K20201001
        Long firstTradeDateTime = DateUtil.stringToLong(firstTradeDate, DateFormatConstant.format_yyyy_mm_dd);
        //如果是日K,那么前收、收、开、高、低、成交量、成交金额就直接存入日K表即可,因为indexBean就是当前今天最新的行情!
        //如果是周K或月K,那么前收、收、开、高、低、成交量、成交金额需要把indexBean中的数据和aggMap中的合并
        //3.比较周期首个交易日是否<当天交易日并判断是否是周K或者是月K,是才更新如下字段,因为日K不需要更新,直接使用上面获取到的即可
        if (firstTradeDateTime < tradeDateTime && (type.equals(2) || type.equals(3))) {
            //如果走if肯定是周K或月K
            //4.从aggMap中获取周/月K上一次记录的数据：高、低、成交量、成交金额
            //trade_vol,
            //trade_amt
            Map<String, Object> map = aggMap.get(indexCode);
            if (map != null && map.size() > 0) {
                BigDecimal laseHighPrice = new BigDecimal(map.get("high_price").toString());
                BigDecimal laseLowPrice = new BigDecimal(map.get("low_price").toString());
                Long laseTradeVol = Long.parseLong(map.get("trade_vol").toString());
                Long laseTradeAmt = Long.parseLong(map.get("trade_amt").toString());
                //5.当前高、低价格和上一次记录的高、低价格进行比较
                if (laseHighPrice.compareTo(highPrice) == 1) {
                    highPrice = laseHighPrice;
                }
                if (laseLowPrice.compareTo(lowPrice) == -1) {
                    lowPrice = laseLowPrice;
                }
                //6.计算成交量、成交金额
                tradeVolDay += laseTradeVol;
                tradeAmtDay += laseTradeAmt;
                //7.计算均价=成交量/成交金额=tradeAmtDay/tradeVolDay
                if (tradeVolDay != 0) {
                    avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, RoundingMode.HALF_UP);
                }
            }
        }
        //如果不走if肯定是日K,日直接使用indexBean中的数据
        // 8.封装数据为Row对象org.apache.flink.types.Row或使用自定义的Bean对象也可以
        Row row = new Row(13);
        row.setField(0, new Timestamp(new Date().getTime()));
        row.setField(1, tradeDate);
        row.setField(2, indexBean.getIndexCode());
        row.setField(3, indexBean.getIndexName());
        row.setField(4, type);
        row.setField(5, preClosePrice);
        row.setField(6, openPrice);
        row.setField(7, highPrice);
        row.setField(8, lowPrice);
        row.setField(9, closePrice);
        row.setField(10, avgPrice);
        row.setField(11, tradeVolDay);
        row.setField(12, tradeAmtDay);
        return row;
    }
}
