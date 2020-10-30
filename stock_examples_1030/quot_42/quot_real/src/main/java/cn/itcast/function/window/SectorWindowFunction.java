package cn.itcast.function.window;

import cn.itcast.bean.SectorBean;
import cn.itcast.bean.StockBean;
import cn.itcast.util.DBUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/10/27 15:01
 * Desc
 * 行业板块业务核心处理窗口函数: StockBean个股转为SectorBean板块
 * AllWindowFunction<IN, OUT, W extends Window>
 */
public class SectorWindowFunction extends RichAllWindowFunction<StockBean, SectorBean, TimeWindow> {
    //开发步骤
    //0.定义变量和状态
    //0.定义基准价(作为上市首日的前一交易日收盘价)
    BigDecimal basePrice = new BigDecimal(1000);

    //0.定义集合用来存放MySQL表中数据
    //Map<板块代码, List<Map<个股字段名, 个股字段值>>>
    Map<String, List<Map<String, Object>>> sectorStockMap = null;

    //Map<板块代码, Map<板块日K字段, 板块日K字段值>>
    Map<String, Map<String, Object>> sectorKlineMap = null;

    //0.定义状态存储上一个窗口板块数据
    //MapState<板块代码, SectorBean>
    MapState<String, SectorBean> sectorState = null;

    //0.在open方法中初始化集合和状态
    @Override
    public void open(Configuration parameters) throws Exception {
        //从行业板块个股组成表中查询数据并封装为sectorStockMap
        String sql1 = "SELECT * FROM bdp_sector_stock WHERE sec_abbr = 'ss'";
        sectorStockMap = DBUtil.queryForGroup("sector_code", sql1);

        //从行业板块日K表中查询数据并封装为sectorKlineMap--现在没有做K线业务,没关系,后续使用时加个判断即可
        String sql2 = "SELECT * FROM bdp_quot_sector_kline_day\n" +
                "WHERE trade_date = (SELECT last_trade_date FROM tcc_date WHERE trade_date = CURDATE())";
        sectorKlineMap = DBUtil.query("sector_code", sql2);

        //创建状态MapState<板块代码, SectorBean >:用来存储上一个窗口板块数据
        MapStateDescriptor<String, SectorBean> descriptor = new MapStateDescriptor<>("sectorMs", String.class, SectorBean.class);
        sectorState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<StockBean> iterable, Collector<SectorBean> collector) throws Exception {
        //开发步骤
        //TODO 1.遍历窗口内个股数据并缓存到新建的Map中HashMap<String, StockBean> cacheStockMap = new HashMap<>();
        //Map<个股代码, StockBean>
        Map<String, StockBean> cacheStockMap = new HashMap<>();
        for (StockBean stockBean : iterable) {
            cacheStockMap.put(stockBean.getSecCode(), stockBean);
        }

        //TODO 2.遍历板块对应关系表下的个股数据、并获取指定板块下的个股
        for (String sectorCode : sectorStockMap.keySet()) {
            //获取指定板块下的个股列表
            List<Map<String, Object>> listStock = sectorStockMap.get(sectorCode);
            //TODO 3.初始化SectorBean中需要的数据
            Long eventTime = 0l;//事件时间--ok
            String sectorName = null;//板块名称--ok
            BigDecimal preClosePrice = new BigDecimal(0);//前收盘价--ok
            BigDecimal highPrice = new BigDecimal(0);//当日最高价--ok
            BigDecimal openPrice = new BigDecimal(0);//开盘价--ok
            BigDecimal lowPrice = new BigDecimal(0);//当日最低价--ok
            BigDecimal closePrice = new BigDecimal(0);//收盘价/当前最新价--ok
            Long tradeVol = 0L; //秒级/分时成交量= 应该使用当前窗口日总成交量- 上一窗口日总成交量--ok
            Long tradeAmt = 0L; //秒级/分时成交额= 应该使用当前窗口日总成交额- 上一窗口日总成交额--ok
            Long tradeVolDay = 0L; //板块日成交总量--ok
            Long tradeAmtDay = 0L; //板块日成交总额--ok
            Long tradeTime = 0L;//交易时间--ok

            //当前板块以开盘价计算的总流通市值 即 累计各个股开盘流通市值
            BigDecimal stockOpenTotalNegoCap = new BigDecimal(0);
            //当前板块以收盘价计算的总流通市值 即 累计各个股收盘流通市值
            BigDecimal stockCloseTotalNegoCap = new BigDecimal(0);
            //前一日板块总流通市值
            BigDecimal preSectorNegoCap = new BigDecimal(0);

            //TODO 4.轮询板块个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
            for (Map<String, Object> map : listStock) {
                //板块名称
                sectorName = map.get("sector_name").toString();
                //个股代码
                String secCode = map.get("sec_code").toString();
                //个股流通股本
                BigDecimal negoCap = new BigDecimal(map.get("nego_cap").toString());
                //前一日板块总流通市值
                preSectorNegoCap = new BigDecimal(map.get("pre_sector_nego_cap").toString());

                //TODO 5.计算：开盘价/收盘价、累计流通市值、累计交易量和累计交易金额（注意：是个股累计值，所以获取缓存个股数据）
                StockBean stockBean = cacheStockMap.get(secCode);
                if (stockBean != null){//防止当前窗口没有该个股数据
                    eventTime = stockBean.getEventTime();
                    tradeTime = stockBean.getTradeTime();

                    //TODO  当前板块以开盘价计算的总流通市值 即 累计各个股开盘流通市值 = SUM(板块下的各个股开盘价*个股流通股本)
                    BigDecimal stockOpenNegoCap = stockBean.getOpenPrice().multiply(negoCap).setScale(2, RoundingMode.HALF_UP);
                    stockOpenTotalNegoCap.add(stockOpenNegoCap);
                    //TODO  当前板块以收盘价计算的总流通市值 即 累计各个股收盘流通市值 = SUM(板块下的各个股收盘价*个股流通股本)
                    BigDecimal stockCloseNegoCap = stockBean.getClosePrice().multiply(negoCap).setScale(2, RoundingMode.HALF_UP);
                    stockCloseTotalNegoCap.add(stockCloseNegoCap);
                    //TODO  板块下各个股的累计成交量量
                    tradeVolDay += stockBean.getTradeVolDay();
                    //TODO  板块下各个股的累计成交金额
                    tradeAmtDay += stockBean.getTradeAmtDay();
                }
            }

            //TODO 6.判断是否是首日上市
            if (sectorKlineMap != null && sectorKlineMap.get(sectorCode) != null) { //非首日上市
                //非首日,板块前收盘价 = T-1日收盘价
                Map<String, Object> map = sectorKlineMap.get(sectorCode);
                preClosePrice = new BigDecimal(map.get("close_price").toString());
            } else {//首日
                //首日,板块前收盘价 = 基准价
                preClosePrice = basePrice;
            }
            //TODO 7.计算板块开盘价和当前价(收盘价)
            //TODO 当前板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的各个股总流通市值 即 累计各个股开盘流通市值/当前板块前一交易日板块总流通市值
            openPrice = preClosePrice.multiply(stockOpenTotalNegoCap).divide(preSectorNegoCap,2, RoundingMode.HALF_UP).setScale(2, RoundingMode.HALF_UP);
            //TODO 当前板块当前价格 = 板块前收盘价*当前板块以收盘价计算的各个股总流通市值 即 累计各个股收盘流通市值/当前板块前一交易日板块总流通市值
            closePrice = preClosePrice.multiply(stockCloseTotalNegoCap).divide(preSectorNegoCap,2, RoundingMode.HALF_UP).setScale(2, RoundingMode.HALF_UP);

            //TODO 8.初始化板块高低价
            highPrice = closePrice;//close其实当前最新价
            lowPrice = closePrice;

            //TODO 9.从状态中获取上一个窗口的板块数据(高、低、成交量、成交金额)
            SectorBean lastSectorBean = sectorState.get(sectorCode);
            if(lastSectorBean != null){
                BigDecimal lastHighPrice = lastSectorBean.getHighPrice();
                BigDecimal lastLowPrice = lastSectorBean.getLowPrice();
                Long lastTradeVol = lastSectorBean.getTradeVolDay();
                Long lastTradeAmt = lastSectorBean.getTradeAmtDay();

                //TODO 10.计算最高价和最低价（前后窗口比较）
                if(lastHighPrice.compareTo(highPrice) == 1){
                    highPrice = lastHighPrice;
                }
                //和下面等价
               /* if(highPrice.compareTo(lastHighPrice) == -1){
                    highPrice = lastHighPrice;
                }*/
                if(lastLowPrice.compareTo(lowPrice) == -1){
                    lowPrice = lastLowPrice;
                }
                //和下面等价
                /*if(lowPrice.compareTo(lastLowPrice) == 1){
                    lowPrice = lastLowPrice;
                }*/
                //TODO 11.计算秒级/分时成交量和成交金额
                //秒级/分时成交量= 应该使用当前窗口日总成交量 - 上一窗口日总成交量
                tradeVol = tradeVolDay - lastTradeVol;
                tradeAmt = tradeAmtDay - lastTradeAmt;
                //秒级/分时成交额= 应该使用当前窗口日总成交额- 上一窗口日总成交额

                //TODO 12.开盘价与高低价比较--因为刚刚的高低都是和上一窗口比的,现在还需要和开盘价再比一下
                if(openPrice.compareTo(highPrice) == 1){
                    highPrice = openPrice;
                }
                if(openPrice.compareTo(lowPrice) == -1){
                    lowPrice = openPrice;
                }
            }

            //TODO 13.封装结果数据
            SectorBean sectorBean = new SectorBean();
            sectorBean.setEventTime(eventTime);
            sectorBean.setSectorCode(sectorCode);
            sectorBean.setSectorName(sectorName);

            sectorBean.setPreClosePrice(preClosePrice);
            sectorBean.setHighPrice(highPrice);
            sectorBean.setOpenPrice(openPrice);
            sectorBean.setLowPrice(lowPrice);
            sectorBean.setClosePrice(closePrice);

            sectorBean.setTradeVol(tradeVol);
            sectorBean.setTradeAmt(tradeAmt);
            sectorBean.setTradeVolDay(tradeVolDay);
            sectorBean.setTradeAmtDay(tradeAmtDay);

            sectorBean.setTradeTime(tradeTime);

            //TODO 14.收集数据
            collector.collect(sectorBean);

            //TODO 15.缓存当前板块数据到状态中
            sectorState.put(sectorCode,sectorBean);
        }
    }
}
