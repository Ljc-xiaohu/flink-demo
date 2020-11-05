package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 告警Bean
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WarnBaseBean {
    private String secCode;//个股代码
    private BigDecimal preClosePrice;//前收
    private BigDecimal highPrice;//高
    private BigDecimal lowPrice;//低
    private BigDecimal closePrice;//收/最新价
    private Boolean flag = false;//用来标记是否超过阈值
    private Long eventTime;//事件时间
}
