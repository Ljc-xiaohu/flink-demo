package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 指数分时行情
 */
//1.新建MinIndexWindowFunction 窗口函数
public class IndexMinutesWindowFunction extends RichWindowFunction<CleanBean, IndexBean, String, TimeWindow> {
    //2.初始化 MapState<String, IndexBean>
    MapState<String, IndexBean> indexMs = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        indexMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, IndexBean>("indexMs", String.class, IndexBean.class));
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<IndexBean> out) throws Exception {

        /**
         * 开发步骤：
         * 1.新建MinIndexWindowFunction 窗口函数
         * 2.初始化 MapState<String, IndexBean>
         * 3.记录最新指数
         * 4.获取分时成交额和成交数量
         * 5.格式化日期
         * 6.封装输出数据
         * 7.更新MapState
         */
        //3.记录最新指数
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            if(line.getEventTime() < cleanBean.getEventTime()){
                line = cleanBean;
            }
        }

        //4.获取分时成交额和成交数量
        Long minVol = 0L;
        Long minAmt = 0L;
        IndexBean indexBeanLast = indexMs.get(line.getSecCode());
        if(indexBeanLast != null){
            Long tradeVolDayLast = indexBeanLast.getTradeVolDay();
            Long tradeAmtDayLast = indexBeanLast.getTradeAmtDay();
            minVol = line.getTradeVolumn() - tradeVolDayLast;
            minAmt = line.getTradeAmt() - tradeAmtDayLast;
        }

        //5.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(line.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);

        //6.封装输出数据
        IndexBean indexBean = new IndexBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                line.getPreClosePx(),
                line.getOpenPrice(),
                line.getMaxPrice(),
                line.getMinPrice(),
                line.getTradePrice(),
                minVol, minAmt,
                line.getTradeVolumn(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        );
        out.collect(indexBean);
        //7.更新MapState
        indexMs.put(indexBean.getIndexCode(),indexBean);
    }
}
