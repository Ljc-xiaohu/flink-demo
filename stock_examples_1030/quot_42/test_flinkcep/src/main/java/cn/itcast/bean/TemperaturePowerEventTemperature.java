package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 封装电压数据实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperaturePowerEventTemperature extends TemperatureMonitoringEvent {
    private double voltage;//power //电压，温度
    private long timestamp; //时间戳

    public TemperaturePowerEventTemperature(int rackID, double voltage, long timestamp) {
        super(rackID,timestamp);
        this.voltage = voltage;
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "PowerEvent(" + getRackID() + ", " + voltage + ", " + timestamp + ")";
    }
}