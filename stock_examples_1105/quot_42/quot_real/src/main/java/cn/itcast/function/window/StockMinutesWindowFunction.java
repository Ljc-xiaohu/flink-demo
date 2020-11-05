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
 * Author itcast
 * Date 2020/10/26 17:16
 * Desc
 * WindowFunction<IN, OUT, KEY, W extends Window>
 * 分时/分级窗口和之前的秒级窗口类似,也是把当前窗口最新的CleanBean转为StockBean并收集
 * 但是分时窗口的StockBea中需要计算最近一分钟内的当前个股的成交量和成交金额
 * 那么当前窗口/当前这1min的成交量或成交金额 =当前窗口最新日总成交量或成交金额 - 上一个窗口最新的日总成交量或成交金额
 * 所以这里需要搞一个地方存储上一个窗口的最新成交量或成交金额---MapState中就可以存储
 */
public class StockMinutesWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {
    //开发步骤：
    //1.定义MapState<股票代码, StockBean>用来缓存上一窗口的数据
    MapState<String, StockBean> stockState = null;

    //2.初始化MapState
    @Override
    public void open(Configuration parameters) throws Exception {
        //定义状态描述器
        MapStateDescriptor<String, StockBean> stateDescriptor = new MapStateDescriptor<>("stockMs", String.class, StockBean.class);
        //根据状态描述器初始化状态
        stockState = getRuntimeContext().getMapState(stateDescriptor);
    }

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> iterable, Collector<StockBean> collector) throws Exception {
        //3.获取分时成交金额或成交量 = 当前窗口的总成交金额或成交量 - 上一窗口的总成交金额或成交量 --->MapState
        //3.1记录最新个股
        CleanBean newCleanBean = null;
        for (CleanBean cleanBean : iterable) {
            //第一次给newCleanBean赋值
            if (newCleanBean == null){
                newCleanBean = cleanBean;
            }
            //后续每次遍历都判断当前进来的cleanBean的EventTime是否比newCleanBean的EventTime大
            //如果是说明当前的cleanBean是最新的newCleanBean
            if(cleanBean.getEventTime() > newCleanBean.getEventTime()){
                newCleanBean = cleanBean;
            }
        }

        //3.2取上一窗口的数据
        StockBean lastStockBean = stockState.get(newCleanBean.getSecCode());
        //3.3定义变量用来记录分时成交额和成交量
        Long minutesVol = 0L;//分时成交量
        Long minutesAmt = 0L;//分时成交金额
        if(lastStockBean != null){
            //3.4获取上一窗口的成交额和成交量
            Long tradeVolDay = lastStockBean.getTradeVolDay();
            Long tradeAmtDay = lastStockBean.getTradeAmtDay();

            //3.5获取目前数据
            Long tradeVolumn = newCleanBean.getTradeVolumn();
            Long tradeAmt = newCleanBean.getTradeAmt();

            //3.6获取分时成交额和成交量
            //当前窗口/当前这1min的成交量或成交金额 =当前窗口最新日总成交量或成交金额 - 上一个窗口最新的日总成交量或成交金额
            minutesVol = tradeVolumn - tradeVolDay;
            minutesAmt = tradeAmt - tradeAmtDay;
        }
        //3.7.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(newCleanBean.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);

        //3.8.封装输出数据
        StockBean stockBean = new StockBean();
        stockBean.setEventTime(newCleanBean.getEventTime());
        stockBean.setSecCode(newCleanBean.getSecCode());
        stockBean.setSecName(newCleanBean.getSecName());
        stockBean.setPreClosePrice(newCleanBean.getPreClosePx());
        stockBean.setOpenPrice(newCleanBean.getOpenPrice());
        stockBean.setHighPrice(newCleanBean.getMaxPrice());
        stockBean.setLowPrice(newCleanBean.getMinPrice());
        stockBean.setClosePrice(newCleanBean.getTradePrice());

        stockBean.setTradeVol(minutesVol);
        stockBean.setTradeAmt(minutesAmt);//秒级行情不需要计算最近5s有多少成交量和成交金额,分时行情才需要

        stockBean.setTradeVolDay(newCleanBean.getTradeVolumn());
        stockBean.setTradeAmtDay(newCleanBean.getTradeAmt());
        stockBean.setTradeTime(tradeTime);
        stockBean.setSource(newCleanBean.getSource());
        //System.out.println(newCleanBean.getSource());

        //3.9收集数据
        collector.collect(stockBean);

        //3.10.更新MapState
        stockState.put(stockBean.getSecCode(),stockBean);
    }
}
