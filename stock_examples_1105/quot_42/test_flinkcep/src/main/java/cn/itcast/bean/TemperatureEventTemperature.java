package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 封装监控温度数据实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperatureEventTemperature extends TemperatureMonitoringEvent {
    private double temperature; //温度
    private long timestamp;  //时间

    public TemperatureEventTemperature(int rackID, double temperature, long timestamp) {
        super(rackID,timestamp);
        this.temperature = temperature;
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "TemperatureEvent(" + getRackID() + ", " + temperature + ", "+timestamp+")";
    }
}