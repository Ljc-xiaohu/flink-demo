package cn.itcast.function.map;

import cn.itcast.bean.StockBean;
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
 * 个股K线数据封装为Row对象:日K、周K、月K通用
 */
public class StockKlineMapFunction extends RichMapFunction<StockBean, Row> {

    /**
     * 开发步骤：
     * 一、初始化
     * 1.创建构造方法
     * 入参：kType：K线类型
     * firstTxdate：周期首个交易日
     * 2.获取交易日历表交易日数据
     * 3.获取周期首个交易日和T日
     * 4.获取K线下的汇总表数据（高、低、成交量、金额）
     */

    private String kType; //k线类型
    private String firstTxdate;//周期内，首个交易日

    //1.创建构造方法
    public StockKlineMapFunction(String kType, String firstTxdate) {
        this.kType = kType;
        this.firstTxdate = firstTxdate;
    }

    String firstTradeDate = null;
    String tradeDate = null;
    Map<String, Map<String, Object>> aggMap = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //2.获取交易日历表交易日数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date = CURDATE()";
        Map<String, String> tradeDateMap = DBUtil.queryKv(sql);

        //3.获取周期首个交易日和T日
        //获取周期首个交易日
        firstTradeDate = tradeDateMap.get(firstTxdate);
        //获取T日
        tradeDate = tradeDateMap.get("trade_date");

        //4.获取K线下的汇总表数据（高、低、量、金额）
        /*String sqlAgg = "SELECT sec_code ,MAX(high_price) AS high_price,MIN(low_price) AS low_price,SUM(trade_vol) AS trade_vol,\n" +
                "SUM(trade_amt) AS trade_amt FROM bdp_quot_stock_kline_day \n" +
                "WHERE trade_date BETWEEN  '" + firstTradeDate + "' AND '" + tradeDate + "'\n" +
                "GROUP BY sec_code";*/

/*
SELECT
    sec_code,
    MAX(high_price) AS high_price,
    MIN(low_price) AS low_price,
    SUM(trade_vol) AS trade_vol,
    SUM(trade_amt) AS trade_amt
FROM
    bdp_quot_stock_kline_day
WHERE
    trade_date BETWEEN '2020-09-28' AND '2020-09-28'
GROUP BY
    sec_code
 */
        String sqlAgg = "SELECT\n" +
                "    sec_code,\n" +
                "    MAX(high_price) AS high_price,\n" +
                "    MIN(low_price) AS low_price,\n" +
                "    SUM(trade_vol) AS trade_vol,\n" +
                "    SUM(trade_amt) AS trade_amt\n" +
                "FROM\n" +
                "    bdp_quot_stock_kline_day\n" +
                "WHERE\n" +
                "    trade_date BETWEEN '" + firstTradeDate + "' AND '" + tradeDate + "'\n" +
                "GROUP BY\n" +
                "    sec_code";

        aggMap = DBUtil.query("sec_code", sqlAgg);
    }

    @Override
    public Row map(StockBean value) throws Exception {
        /**
         * 开发步骤：
         * 1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
         * 2.获取T日和周首次交易日时间,转换成long型
         * 3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
         * 4.获取周/月K数据：成交量、成交额、高、低
         * 5.高、低价格比较
         * 6.计算成交量、成交额
         * 7.计算均价
         * 8.封装数据Row
         */
        //1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
        BigDecimal preClosePrice = value.getPreClosePrice();
        BigDecimal closePrice = value.getClosePrice();
        BigDecimal openPrice = value.getOpenPrice();
        BigDecimal highPrice = value.getHighPrice();
        BigDecimal lowPrice = value.getLowPrice();
        Long tradeVolDay = value.getTradeVolDay();
        Long tradeAmtDay = value.getTradeAmtDay();

        // 2.获取T日和周期首次交易日时间,转换成long型
        Long tradeDateTime = DateUtil.stringToLong(tradeDate, DateFormatConstant.format_yyyy_mm_dd);
        Long firstTradeDateTime = DateUtil.stringToLong(firstTradeDate, DateFormatConstant.format_yyyy_mm_dd);
        //判断是否是周K或者是月K,true才更新如下字段,因为日K不需要更新,直接使用上面获取到的即可
        if (firstTradeDateTime < tradeDateTime && (kType.equals("2") || kType.equals("3"))) {
            // 4.获取周/月K数据：成交量、成交额、高、低
            Map<String, Object> map = aggMap.get(value.getSecCode());
            if (map != null && map.size() > 0) {
                BigDecimal highPriceLast = new BigDecimal(map.get("high_price").toString());
                BigDecimal lowPriceLast = new BigDecimal(map.get("low_price").toString());
                Long tradeAmtLast = Double.valueOf(map.get("trade_amt").toString()).longValue();
                Long tradeVolLast = Double.valueOf(map.get("trade_vol").toString()).longValue();

                //5.高、低价格比较
                if (highPrice.compareTo(highPriceLast) == -1) {
                    highPrice = highPriceLast;
                }
                if (lowPrice.compareTo(lowPriceLast) == 1) {
                    lowPrice = lowPriceLast;
                }

                //6.计算成交量、成交额
                tradeAmtDay += tradeAmtLast;
                tradeVolDay += tradeVolLast;
            }
        }
        // 7.计算均价
        BigDecimal avgPrice = new BigDecimal(0);
        if (tradeVolDay != 0) {
            avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, RoundingMode.HALF_UP);
        }

        //8.封装数据Row
        Row row = new Row(13);
        row.setField(0, new Timestamp(new Date().getTime()));
        row.setField(1, tradeDate);
        row.setField(2, value.getSecCode());
        row.setField(3, value.getSecName());
        row.setField(4, kType);
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
