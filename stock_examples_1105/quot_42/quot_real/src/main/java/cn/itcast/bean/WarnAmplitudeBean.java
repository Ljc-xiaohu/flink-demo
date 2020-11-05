package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 振幅bean
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WarnAmplitudeBean {
    private String secCode;
    private BigDecimal preClosePrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private Boolean flag = false;//用来标记是否超过阈值
}
