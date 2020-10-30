package cn.itcast.function.sink;

import cn.itcast.util.HBaseUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * Author itcast
 * Date 2020/10/26 16:17
 * Desc 调用工具类将List<Put>批量保存到HBase
 */
public class HBaseSink implements SinkFunction<List<Put>> {
    private String tableName;
    public HBaseSink(String tableName){
        this.tableName = tableName;
    }
    @Override
    public void invoke(List<Put> value, Context context) throws Exception {
        //调用工具类把List<Put>存入HBase
        HBaseUtil.putList(tableName,value);
        System.out.println("数据已批量写入到HBase");
    }
}
