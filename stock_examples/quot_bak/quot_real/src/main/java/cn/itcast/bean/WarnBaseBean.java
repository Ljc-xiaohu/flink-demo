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
public class WarnBaseBean {
    private String secCode;
    private BigDecimal preClosePrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;
    private Long eventTime;
}
