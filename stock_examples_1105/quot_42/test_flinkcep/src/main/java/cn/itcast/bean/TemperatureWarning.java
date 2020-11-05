package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 告警实体类
 */
@Data
@AllArgsConstructor
public class TemperatureWarning {
    private int rackID;//机架id
    private double averageTemperature;//平均温度
    public TemperatureWarning() {
        this(-1, -1);
    }
}