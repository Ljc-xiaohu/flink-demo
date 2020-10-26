package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncreaseBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 */
//1.新建MinStockIncrWindowFunction 窗口函数
public class StockMinutesIncreaseWindowFunction extends RichWindowFunction<CleanBean, StockIncreaseBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockIncreaseBean> out) throws Exception {
        /**
         * 开发步骤：
         * 1.新建MinStockIncrWindowFunction 窗口函数
         * 2.记录最新个股
         * 3.格式化日期
         * 4.指标计算
         *   涨跌、涨跌幅、振幅
         * 5.封装输出数据
         */
        //2.记录最新个股
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if (line == null) {
                line = cleanBean;
            }
            if (line.getEventTime() < cleanBean.getEventTime()) {
                line = cleanBean;
            }
        }
        //3.格式化日期
        Long tradeTime = DateUtil.longTimestamp2LongFormat(line.getEventTime(), DateFormatConstant.format_YYYYMMDDHHMMSS);
        //4.指标计算:涨跌幅、振幅
        //今日涨跌 = 当前价-前收盘价
        BigDecimal updown = line.getTradePrice().subtract(line.getPreClosePx());
        //今日涨跌幅% = 今日涨跌 / 前收盘价 * 100%
        BigDecimal increase = updown.divide(line.getPreClosePx(), 2, RoundingMode.HALF_UP);//保留两位小数，四舍五入
        //今日振幅 %=（当日最高点的价格－当日最低点的价格）/ 昨天收盘价 × 100%
        BigDecimal amplitude = (line.getMaxPrice().subtract(line.getMinPrice())).divide(line.getPreClosePx(), 2, RoundingMode.HALF_UP);
        //5.封装输出数据
        StockIncreaseBean stockIncreaseBean = new StockIncreaseBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                increase,
                line.getTradePrice(),
                updown,
                line.getTradeVolumn(),
                amplitude,
                line.getPreClosePx(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        );
        out.collect(stockIncreaseBean);
    }
}
