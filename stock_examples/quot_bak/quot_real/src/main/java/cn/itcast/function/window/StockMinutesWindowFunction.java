package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 个股分时行情
 */
//1.新建MinStockWindowFunction 窗口函数
public class StockMinutesWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {
    //缓存上一窗口的数据
    MapState<String, StockBean> stockMs = null;

    //2.初始化 MapState<String, StockBean>
    @Override
    public void open(Configuration parameters) throws Exception {
        stockMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockBean>("stockMs", String.class, StockBean.class));
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        /**
         * 开发步骤：
         * 获取分时成交金额或量 = 当前窗口的总成交金额或量 - 上一窗口的总成交金额或量->MapState
          */
        //3.记录最新个股
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            if(line.getEventTime()< cleanBean.getEventTime()){
                line= cleanBean;
            }
        }

        //4.获取分时成交额和成交数量
        StockBean stockBeanLast = stockMs.get(line.getSecCode());
        Long minVol = 0L;//分时成交量
        Long minAmt = 0L;//分时成交金额
        if(stockBeanLast !=null){
            //上一窗口的数据
            Long tradeAmtDayLast = stockBeanLast.getTradeAmtDay();
            Long tradeVolDayLast = stockBeanLast.getTradeVolDay();
            //当前窗口的数据
            Long tradeAmtDay = line.getTradeAmt();
            Long tradeVolumnDay = line.getTradeVolumn();
            minVol = tradeVolumnDay - tradeVolDayLast;
            minAmt = tradeAmtDay - tradeAmtDayLast;
        }

        //5.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(line.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);
        //6.封装输出数据
        StockBean stockBean = new StockBean();
        stockBean.setEventTime(line.getEventTime());
        stockBean.setSecCode(line.getSecCode());
        stockBean.setSecName(line.getSecName());
        stockBean.setPreClosePrice(line.getPreClosePx());
        stockBean.setOpenPrice(line.getOpenPrice());
        stockBean.setHighPrice(line.getMaxPrice());
        stockBean.setLowPrice(line.getMinPrice());
        stockBean.setClosePrice(line.getTradePrice());

        stockBean.setTradeVol(minVol);
        stockBean.setTradeAmt(minAmt);

        stockBean.setTradeVolDay(line.getTradeVolumn());
        stockBean.setTradeAmtDay(line.getTradeAmt());
        stockBean.setTradeTime(tradeTime);
        stockBean.setSource(line.getSource());
        out.collect(stockBean);
        //7.更新MapState
        stockMs.put(line.getSecCode(),stockBean);

    }
}
