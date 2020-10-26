package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TurnoverRateBean {
    private String secCode;
    private String secName;
    private BigDecimal tradePrice;
    private Long tradeVol;
    private BigDecimal negoCap;//个股流通股本
}
