package cn.itcast.function.window;

import cn.itcast.bean.StockBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * 封装list<Put></>
 */
public class StockPutHBaseWindowFunction extends RichAllWindowFunction<StockBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<StockBean> values, Collector<List<Put>> out) throws Exception {

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
        for (StockBean value : values) {
            //3.设置rowkey
            String rowkey = value.getSecCode()+value.getTradeTime();
            //4.json数据转换
            String jsonString = JSON.toJSONString(value);
            // 5.封装put
            Put put = new Put(rowkey.getBytes());
            put.addColumn("info".getBytes(),"data".getBytes(),jsonString.getBytes());
            list.add(put);
        }

        // 6.收集数据
        out.collect(list);
    }
}
