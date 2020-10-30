package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 告警实体类
 */
@Data
@AllArgsConstructor
public class TemperatureAlert {
    private int rackID; //机架id
    public TemperatureAlert() {
        this(-1);
    }
}
