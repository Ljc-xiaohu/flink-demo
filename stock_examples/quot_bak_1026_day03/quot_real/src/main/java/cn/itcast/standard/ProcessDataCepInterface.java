package cn.itcast.standard;

import cn.itcast.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 */
public interface ProcessDataCepInterface {
    void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env);
}
