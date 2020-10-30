package cn.itcast.function.window;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.constant.DateFormatConstant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Author itcast
 * Date 2020/10/26 15:41
 * Desc
 */
//WindowFunction<IN, OUT, KEY, W extends Window>
//WindowFunction<CleanBean, IndexBean, String, TimeWindow>
//把当前窗口/5s内最新的CleanBean转为IndexBean
public class IndexSecondsWindowFunction implements WindowFunction<CleanBean, IndexBean,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> iterable, Collector<IndexBean> collector) throws Exception {
        //注意:
        //进来的Iterable<CleanBean> iterable包含该指数窗口5s的所有数据,
        //而业务需求其实需要在前台页面每隔5s显示一条最新的行情数据
        //1.记录最新指数
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
        //3.封装输出数据
        IndexBean indexBean = new IndexBean();
        indexBean.setEventTime(newCleanBean.getEventTime());
        indexBean.setIndexCode(newCleanBean.getSecCode());
        indexBean.setIndexName(newCleanBean.getSecName());
        indexBean.setPreClosePrice(newCleanBean.getPreClosePx());
        indexBean.setOpenPrice(newCleanBean.getOpenPrice());
        indexBean.setHighPrice(newCleanBean.getMaxPrice());
        indexBean.setLowPrice(newCleanBean.getMinPrice());
        indexBean.setClosePrice(newCleanBean.getTradePrice());
        indexBean.setTradeVol(0L);
        indexBean.setTradeAmt(0L);//秒级行情不需要计算最近5s有多少成交量和成交金额,分时行情才需要
        indexBean.setTradeVolDay(newCleanBean.getTradeVolumn());
        indexBean.setTradeAmtDay(newCleanBean.getTradeAmt());
        indexBean.setTradeTime(tradeTime);
        indexBean.setSource(newCleanBean.getSource());

        //4.收集数据
        collector.collect(indexBean);
    }
}
