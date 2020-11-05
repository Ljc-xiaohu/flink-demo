package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 首先我们定义一个监控事件实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class TemperatureMonitoringEvent {
    private int rackID;
    private long timestamp; //时间戳
}