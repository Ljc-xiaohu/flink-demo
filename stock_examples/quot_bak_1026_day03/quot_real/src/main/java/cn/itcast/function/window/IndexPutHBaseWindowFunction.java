package cn.itcast.function.window;

import cn.itcast.bean.IndexBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class IndexPutHBaseWindowFunction extends RichAllWindowFunction<IndexBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<IndexBean> values, Collector<List<Put>> out) throws Exception {
        /**
         * 开发步骤：
         * 1.新建List<Put>
         * 2.循环数据
         * 3.设置rowkey
         * 4.json数据转换
         * 5.封装put
         * 6.收集数据
         */
        //1.新建List<Put>
        List<Put> list = new ArrayList<>();
        //2.循环数据
        for (IndexBean line : values) {
            //3.设置rowkey
            String rowkey = line.getIndexCode()+line.getTradeTime();
            //5.封装put
            Put put = new Put(rowkey.getBytes());
            put.addColumn("info".getBytes(),"data".getBytes(), JSON.toJSONString(line).getBytes());
            list.add(put);
        }

        //6.收集数据
        out.collect(list);
    }
}
