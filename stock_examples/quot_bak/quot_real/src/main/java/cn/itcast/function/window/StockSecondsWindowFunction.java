package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 秒级行情窗口函数
 */
//1.新建SecStockWindowFunction 窗口函数
public class StockSecondsWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        //2.记录最新个股
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            //时间判断
            if(line.getEventTime() < cleanBean.getEventTime()){
                line = cleanBean;
            }
        }

        //3.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(line.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);

        //4.封装输出数据
        StockBean stockBean = new StockBean();
        stockBean.setEventTime(line.getEventTime());
        stockBean.setSecCode(line.getSecCode());
        stockBean.setSecName(line.getSecName());
        stockBean.setPreClosePrice(line.getPreClosePx());
        stockBean.setOpenPrice(line.getOpenPrice());
        stockBean.setHighPrice(line.getMaxPrice());
        stockBean.setLowPrice(line.getMinPrice());
        stockBean.setClosePrice(line.getTradePrice());
        stockBean.setTradeVol(0l);
        stockBean.setTradeAmt(0l);//秒级行情不包含分时成交数据
        stockBean.setTradeVolDay(line.getTradeVolumn());
        stockBean.setTradeAmtDay(line.getTradeAmt());
        stockBean.setTradeTime(tradeTime);
        stockBean.setSource(line.getSource());

        out.collect(stockBean);
    }
}
