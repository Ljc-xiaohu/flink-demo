package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 板块行情
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SectorBean {
    private Long eventTime;//事件时间
    private String sectorCode;//板块代码
    private String sectorName;//板块名称
    private BigDecimal preClosePrice;//昨收盘价
    private BigDecimal openPrice;//开盘价
    private BigDecimal highPrice;//当日最高价
    private BigDecimal lowPrice;//当日最低价
    private BigDecimal closePrice;//当前价
    private Long tradeVol;//当前成交量
    private Long tradeAmt;//当前成交额
    private Long tradeVolDay;//日成交量
    private Long tradeAmtDay;//日成交额
    private Long tradeTime;//交易时间
}
