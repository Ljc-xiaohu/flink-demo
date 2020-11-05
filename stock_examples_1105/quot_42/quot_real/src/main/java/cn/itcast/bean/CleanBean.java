package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 数据清洗对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CleanBean {
    private String mdStreamId ; //行情类型
    private String secCode ;//个股代码
    private String secName;//个股名称
    private Long tradeVolumn;//日成交总数量
    private Long tradeAmt;//日成交总金额
    private BigDecimal preClosePx; //前收盘价
    private BigDecimal openPrice ;//开盘价
    private BigDecimal maxPrice;//最高价
    private BigDecimal minPrice;//最低价
    private BigDecimal tradePrice;//最新价
    private Long eventTime;//事件时间
    private String source ;//数据来源
}
