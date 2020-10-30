package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncreaseBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Author itcast
 * Date 2020/10/27 10:16
 * Desc
 * WindowFunction<IN, OUT, KEY, W extends Window>
 * 计算各个涨跌幅并转为StockIncreaseBean收集
 */
public class StockMinutesIncreaseWindowFunction implements WindowFunction<CleanBean, StockIncreaseBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> iterable, Collector<StockIncreaseBean> collector) throws Exception {
        //开发步骤：
        //1.记录最新个股
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

        //2.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(newCleanBean.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);

        //3.指标计算:涨跌幅、振幅
        //注意:Bigdecimal除法的精度设置需要在divide方法内部设置
        //今日涨跌 = 当前价-前收盘价(结果>0为涨,结果<0为跌)
        BigDecimal updown = (newCleanBean.getTradePrice()).subtract(newCleanBean.getPreClosePx());
        //今日涨跌幅% = 今日涨跌 / 前收盘价 * 100%
        BigDecimal increase = updown.divide(newCleanBean.getPreClosePx(),2, RoundingMode.HALF_UP);
        //今日振幅%=(当日最高点的价格－当日最低点的价格)/ 前收盘价 × 100%
        BigDecimal amplitude = (newCleanBean.getMaxPrice().subtract(newCleanBean.getMinPrice()))
                .divide(newCleanBean.getPreClosePx(),2, RoundingMode.HALF_UP);

        //4.封装输出数据
        StockIncreaseBean stockIncreaseBean = new StockIncreaseBean(
                newCleanBean.getEventTime(),
                newCleanBean.getSecCode(),
                newCleanBean.getSecName(),
                increase,
                newCleanBean.getTradePrice(),
                updown,
                newCleanBean.getTradeVolumn(),
                amplitude,
                newCleanBean.getPreClosePx(),
                newCleanBean.getTradeAmt(),
                tradeTime,
                newCleanBean.getSource()
        );

        //5.收集数据
        collector.collect(stockIncreaseBean);
    }
}
