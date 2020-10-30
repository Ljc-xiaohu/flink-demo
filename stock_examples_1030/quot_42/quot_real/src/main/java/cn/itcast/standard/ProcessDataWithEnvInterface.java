package cn.itcast.standard;

import cn.itcast.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author itcast
 * Date 2020/10/26 15:30
 * Desc
 * 定义一个数据处理接口/规范
 * 所有的数据处理业务都需要实现该接口/规范!
 */
public interface ProcessDataWithEnvInterface {
    void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env);
}
