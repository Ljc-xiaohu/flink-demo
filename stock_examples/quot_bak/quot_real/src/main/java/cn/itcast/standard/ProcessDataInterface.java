package cn.itcast.standard;

import cn.itcast.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 实时业务数据处理规范接口
 */
public interface ProcessDataInterface {
    void process(DataStream<CleanBean> waterData);
}
