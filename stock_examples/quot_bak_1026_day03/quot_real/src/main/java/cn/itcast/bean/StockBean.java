package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 个股行情
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockBean {
    private Long eventTime; //事件时间
    private String secCode;//证券代码
    private String secName;//证券名称
    private BigDecimal preClosePrice;//昨收盘价
    private BigDecimal openPrice;//开盘价
    private BigDecimal highPrice;//当日最高价
    private BigDecimal lowPrice;//当日最低价
    private BigDecimal closePrice;//当前价
    private Long tradeVol;//当前成交量/分时成交量
    private Long tradeAmt;//当前成交额/分时成交金额
    private Long tradeVolDay;//日成交总量
    private Long tradeAmtDay;//日成交总金额
    private Long tradeTime; //交易时间/格式化时间
    private String source;//数据来源
}
