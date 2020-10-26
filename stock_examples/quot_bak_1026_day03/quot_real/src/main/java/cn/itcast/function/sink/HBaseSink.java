package cn.itcast.function.sink;

import cn.itcast.util.HBaseUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 */
public class HBaseSink extends RichSinkFunction<List<Put>> {
    //开发步骤：
    //1.创建构造方法
    private String tableName;
    public HBaseSink(String tableName){
        this.tableName = tableName;
    }

    //2.执行写入操作
    @Override
    public void invoke(List<Put> value, Context context) throws Exception {
        //执行数据写入
        HBaseUtil.putList(tableName,value);
    }
}
