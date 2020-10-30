package cn.itcast.function.window;

import cn.itcast.bean.StockBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * Author itcast
 * Date 2020/10/26 16:05
 * Desc
 * 将当前窗口内的数据封装为 List<Put>, 目的是为了后续将数据批量写入到HBase
 * AllWindowFunction<IN, OUT, W extends Window>
 * AllWindowFunction<StockBean, List<Put>, TimeWindow>
 */
public class StockPutHBaseWindowFunction implements AllWindowFunction<StockBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<StockBean> iterable, Collector<List<Put>> collector) throws Exception {
        //把当前窗口的数据Iterable<StockBean> iterable封装为List<Put>并收集
        //1.准备List<Put>
        List<Put> list =  new ArrayList<>();
        //2.遍历iterable
        for (StockBean stockBean : iterable) {
            //3.封装Put
            String rowkey = stockBean.getSecCode() + stockBean.getTradeTime();
            String jsonString = JSON.toJSONString(stockBean);
            Put put = new Put(rowkey.getBytes());
            put.addColumn("info".getBytes(), "data".getBytes(), jsonString.getBytes());
            //上面代码表示把stockBean转为json并存到hbase中,列族为info,列名为data,rowkey为股票代码+交易时间
            //4.put加入到List
            list.add(put);
        }
        //5.收集
        collector.collect(list);
    }
}
