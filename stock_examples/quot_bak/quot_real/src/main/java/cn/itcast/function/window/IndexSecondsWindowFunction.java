package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 */
public class IndexSecondsWindowFunction extends RichWindowFunction<CleanBean, IndexBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<IndexBean> out) throws Exception {
        /**
         * 开发步骤：
         * 1.新建SecIndexWindowFunction 窗口函数
         * 2.记录最新指数
         * 3.格式化日期
         * 4.封装输出数据
         */
        // 2.记录最新指数
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            if(line.getEventTime()<cleanBean.getEventTime()){
                line= cleanBean;
            }
        }

        //3.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(line.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);
        //4.封装输出数据
        IndexBean indexBean = new IndexBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                line.getPreClosePx(),
                line.getOpenPrice(),
                line.getMaxPrice(),
                line.getMinPrice(),
                line.getTradePrice(),
                0L, 0L,
                line.getTradeVolumn(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        );
        //数据收集
        out.collect(indexBean);
    }
}
