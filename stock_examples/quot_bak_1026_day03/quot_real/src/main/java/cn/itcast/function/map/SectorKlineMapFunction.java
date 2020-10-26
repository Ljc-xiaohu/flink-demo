package cn.itcast.function.map;

import cn.itcast.bean.SectorBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.DBUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * 板块K线数据封装为Row对象:日K、周K、月K通用
 */
public class SectorKlineMapFunction extends RichMapFunction<SectorBean, Row> {

    String kType; //K线类型
    String firstTxDate; //周期内的首个交易日

    public SectorKlineMapFunction(String kType, String firstTxDate) {
        this.kType = kType;
        this.firstTxDate = firstTxDate;
    }

    String firstTradeDate;
    String tradeDate; //当前日期
    Map<String, Map<String, Object>> aggMap = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        /*
         初始化
         1.创建构造方法
           入参：kType：K线类型
                 firstTxdate：周期首个交易日
         2.获取交易日历表交易日数据
         3.获取周期首个交易日和T日
         4.获取K线下的汇总表数据（高、低、成交量、金额）
         */
        //2.获取交易日历表交易日数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date = CURDATE()";
        Map<String, String> tradeDateMap = DBUtil.queryKv(sql);
        //3.获取周期首个交易日和T日
        //获取周期首个交易日
        firstTradeDate = tradeDateMap.get(firstTxDate);
        //获取T日
        tradeDate = tradeDateMap.get("trade_date");

        //4.获取K线下的汇总表数据(高、低、成交量、金额)
/*
SELECT
	sector_code,
	MAX(high_price) AS high_price,
	MIN(low_price) AS low_price,
	SUM(trade_vol) AS trade_vol,
	SUM(trade_amt) AS trade_amt
FROM
	bdp_quot_sector_kline_day
WHERE
	trade_date BETWEEN '2020-07-07' AND '2020-07-07'
GROUP BY
	sector_code
 */
        String sqlAgg = "SELECT\n" +
                "\tsector_code,\n" +
                "\tMAX(high_price) AS high_price,\n" +
                "\tMIN(low_price) AS low_price,\n" +
                "\tSUM(trade_vol) AS trade_vol,\n" +
                "\tSUM(trade_amt) AS trade_amt\n" +
                "FROM\n" +
                "\tbdp_quot_sector_kline_day \n" +
                "WHERE\n" +
                "\ttrade_date BETWEEN '"+firstTradeDate+" ' AND '"+tradeDate+"'\n" +
                "GROUP BY\n" +
                "\tsector_code";
        aggMap = DBUtil.query("sector_code", sqlAgg);
    }

    @Override
    public Row map(SectorBean value) throws Exception {

        /*
         开发步骤：
         1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
         2.获取T日和周首次交易日时间,转换成long型
         3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
         4.获取周/月K数据：成交量、成交额、高、低
         5.高、低价格比较
         6.计算均价
         7.封装数据Row
         */
        //1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
        BigDecimal preClosePrice = value.getPreClosePrice();
        BigDecimal closePrice = value.getClosePrice();
        BigDecimal openPrice = value.getOpenPrice();
        BigDecimal highPrice = value.getHighPrice();
        BigDecimal lowPrice = value.getLowPrice();
        Long tradeVolDay = value.getTradeVolDay();
        Long tradeAmtDay = value.getTradeAmtDay();

        //2.获取T日和周首次交易日时间,转换成long型
        Long tradeTime = DateUtil.stringToLong(tradeDate, DateFormatConstant.format_yyyy_mm_dd);
        Long firstTradeTime = DateUtil.stringToLong(firstTradeDate, DateFormatConstant.format_yyyy_mm_dd);

        //3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
        if (firstTradeTime < tradeTime && (kType.equals("2") || kType.equals("3"))) {
            //是周K或月K
            //4.获取周/月K数据：成交量、成交额、高、低
            Map<String, Object> map = aggMap.get(value.getSectorCode());
            if (null != map && map.size() > 0) {
                //4.获取周/月K数据：成交量、成交额、高、低
                BigDecimal highPriceLast = new BigDecimal(map.get("high_price").toString());
                BigDecimal lowPriceLast = new BigDecimal(map.get("low_price").toString());
                Long tradeAmtLast = Double.valueOf(map.get("trade_amt").toString()).longValue();
                Long tradeVolLast = Double.valueOf(map.get("trade_vol").toString()).longValue();

                //5.高、低价格比较
                if (highPriceLast.compareTo(highPrice) == 1) {
                    highPrice = highPriceLast;
                }
                if (lowPriceLast.compareTo(lowPrice) == -1) {
                    lowPrice = lowPriceLast;
                }

                //6.计算成交量、成交额
                tradeVolDay += tradeVolLast;
                tradeAmtDay += tradeAmtLast;
            }
        }

        //6.计算均价
        BigDecimal avgPrice = new BigDecimal(0);
        if (tradeVolDay != 0) {
            avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, RoundingMode.HALF_UP);
        }

        //7.封装数据Row
        Row row = new Row(13);
        row.setField(0, new Timestamp(new Date().getTime()));
        row.setField(1, tradeDate);
        row.setField(2, value.getSectorCode());
        row.setField(3, value.getSectorName());
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
