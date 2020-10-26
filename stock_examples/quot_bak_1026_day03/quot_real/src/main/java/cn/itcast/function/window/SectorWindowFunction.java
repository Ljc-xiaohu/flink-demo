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
 * 板块秒级行情数据处理-板块业务核心代码
 */
public class SectorWindowFunction extends RichAllWindowFunction<StockBean, SectorBean, TimeWindow> {
    /**
     * open初始化
     * 1. 初始化数据：板块对应关系、最近交易日日K
     * 2. 定义状态MapState<String, SectorBean >:上一个窗口板块数据
     * 3. 初始化基准价
     */
    //0.定义基准价(作为上市首日的前一交易日收盘价)
    BigDecimal basePrice = new BigDecimal(1000);

    //0.定义集合用来存放MySQL表中数据
    //Map<板块代码, List<Map<个股字段, 个股字段值>>>
    Map<String, List<Map<String, Object>>> sectorStockMap = null;
    //Map<板块代码, Map<板块日K字段, 板块日K字段值>>
    Map<String, Map<String, Object>> sectorKlineMap = null;

    //0.定义状态
    MapState<String, SectorBean> sectorMs = null;

    //0.初始化集合和状态
    @Override
    public void open(Configuration parameters) throws Exception {
        //查询bdp_sector_stock表中板块代码和个股的对应关系
        String sql = "SELECT * FROM bdp_sector_stock WHERE sec_abbr = 'ss'";
        sectorStockMap = DBUtil.queryForGroup("sector_code", sql);

        //查询bdp_quot_sector_kline_day表中板块代码和板块日K信息对应关系
        String sql2 = "SELECT * FROM bdp_quot_sector_kline_day\n" +
                "WHERE trade_date = (SELECT last_trade_date FROM tcc_date WHERE trade_date = CURDATE())";
        sectorKlineMap = DBUtil.query("sector_code", sql2);

        //创建状态MapState<String, SectorBean >:上一个窗口板块数据
        sectorMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, SectorBean>("sectorMs", String.class, SectorBean.class));
    }

    /**
     * 核心业务逻辑
     */
    @Override
    public void apply(TimeWindow window, Iterable<StockBean> values, Collector<SectorBean> out) throws Exception {
        /**
         * 开发步骤
         * 1.循环窗口内个股数据并缓存
         * 2.轮询板块对应关系表下的个股数据、并获取指定板块下的个股
         * 3.初始化全部数据
         * 4.轮询板块个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
         * 5.计算：开盘价/收盘价累计流通市值、累计（交易量、交易金额）（注意：是个股累计值，获取缓存个股数据）
         *    累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
         *    累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
         * 6.判断是否是首日上市，并计算板块开盘价和收盘价
         *    板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
         *    板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
         * 7.初始化板块高低价
         * 8.获取上一个窗口板块数据（高、低、成交量、成交金额）
         * 9.计算最高价和最低价（前后窗口比较）
         * 10.计算分时成交量和成交金额
         * 11.开盘价与高低价比较
         * 12.封装结果数据
         * 13.缓存当前板块数据
         */
        //1.循环窗口内个股数据并缓存
        HashMap<String, StockBean> cacheStockMap = new HashMap<>();
        for (StockBean value : values) {
            cacheStockMap.put(value.getSecCode(), value);
        }

        //2.遍历板块对应关系表下的个股数据、并获取指定板块下的个股
        for (String sectorCode : sectorStockMap.keySet()) {
            //并获取指定板块下的个股
            List<Map<String, Object>> listStock = sectorStockMap.get(sectorCode);

            //3.初始化SectorBean中需要的数据
            Long eventTime = 0l;
            String sectorName = null;
            BigDecimal preClosePrice = new BigDecimal(0);
            BigDecimal highPrice = new BigDecimal(0);
            BigDecimal openPrice = new BigDecimal(0);
            BigDecimal lowPrice = new BigDecimal(0);
            BigDecimal closePrice = new BigDecimal(0);
            Long tradeVol = 0L; //分时成交量
            Long tradeAmt = 0L; //分时成交额
            Long tradeVolDay = 0L; //日成交总量
            Long tradeAmtDay = 0L; //日成交总额
            Long tradeTime = 0L;

            //当前板块以开盘价计算的总流通市值 即 累计各个股开盘流通市值
            BigDecimal stockOpenTotalNegoCap = new BigDecimal(0);
            //当前板块以收盘价计算的总流通市值 即 累计各个股收盘流通市值
            BigDecimal stockCloseTotalNegoCap = new BigDecimal(0);
            //前一日板块总流通市值
            BigDecimal preSectorNegoCap = new BigDecimal(0);

            //4.遍历板块个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
            for (Map<String, Object> map : listStock) {
                //板块名称
                sectorName = map.get("sector_name").toString();
                //个股代码
                String secCode = map.get("sec_code").toString();
                //个股流通股本
                BigDecimal negoCap = new BigDecimal(map.get("nego_cap").toString());
                //前一日板块总流通市值
                preSectorNegoCap = new BigDecimal(map.get("pre_sector_nego_cap").toString());

                //5.计算：开盘价/收盘价、累计流通市值、累计交易量和累计交易金额（注意：是个股累计值，获取缓存个股数据）
                StockBean stockBean = cacheStockMap.get(secCode);
                if (stockBean != null) { //非空判断,保证实时流中的个股，一定是存在板块个股对应关系表中的数据，才能够进行累加操作
                    //时间可以直接设置无需计算
                    eventTime = stockBean.getEventTime();
                    tradeTime = stockBean.getTradeTime();

                    //当前板块以开盘价计算的总流通市值 即 累计各个股开盘流通市值 = SUM(板块下的各个股开盘价*个股流通股本)
                    BigDecimal stockNegoCap = stockBean.getOpenPrice().multiply(negoCap).setScale(2, BigDecimal.ROUND_HALF_UP);
                    stockOpenTotalNegoCap = stockOpenTotalNegoCap.add(stockNegoCap);

                    //当前板块以收盘价计算的总流通市值 即 累计各个股收盘流通市值 = SUM(板块下的各个股收盘价*个股流通股本)
                    BigDecimal stockCloseNegoCap = stockBean.getClosePrice().multiply(negoCap).setScale(2, RoundingMode.DOWN);
                    stockCloseTotalNegoCap = stockCloseTotalNegoCap.add(stockCloseNegoCap);

                    //板块下各个股的累计成交量量
                    tradeVolDay += stockBean.getTradeVolDay();
                    //板块下各个股的累计成交金额
                    tradeAmtDay += stockBean.getTradeAmtDay();
                }
            }

            //6.判断是否是首日上市
            if (sectorKlineMap != null && sectorKlineMap.get(sectorCode) != null) {
                //非首日,板块前收盘价 = T-1日收盘价
                Map<String, Object> map = sectorKlineMap.get(sectorCode);
                preClosePrice = new BigDecimal(map.get("close_price").toString());
            } else {
                //首日,板块前收盘价 = 基准价
                preClosePrice = basePrice;
            }
            //7.计算板块开盘价和当前价(收盘价)
            //当前板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的各个股总流通市值 即 累计各个股开盘流通市值/当前板块前一交易日板块总流通市值
            //当前板块当前价格 = 板块前收盘价*当前板块以收盘价计算的各个股总流通市值 即 累计各个股收盘流通市值/当前板块前一交易日板块总流通市值
            openPrice = preClosePrice.multiply(stockOpenTotalNegoCap.divide(preSectorNegoCap, 2, RoundingMode.HALF_UP)).setScale(2, RoundingMode.HALF_UP);
            closePrice = preClosePrice.multiply(stockCloseTotalNegoCap.divide(preSectorNegoCap, 2, RoundingMode.HALF_UP)).setScale(2, RoundingMode.HALF_UP);

            //8.初始化板块高低价
            highPrice = closePrice;
            lowPrice = closePrice;

            //9.从状态中获取上一个窗口的板块数据(高、低、成交量、成交金额)
            SectorBean sectorBeanLast = sectorMs.get(sectorCode);
            BigDecimal highPriceLast = new BigDecimal(0);
            BigDecimal lowPriceLast = new BigDecimal(0);
            Long tradeAmtDayLast = 0L;
            Long tradeVolDayLast = 0L;
            if (sectorBeanLast != null) {
                highPriceLast = sectorBeanLast.getHighPrice();
                lowPriceLast = sectorBeanLast.getLowPrice();
                tradeAmtDayLast = sectorBeanLast.getTradeAmtDay();
                tradeVolDayLast = sectorBeanLast.getTradeVolDay();

                //10.计算最高价和最低价（前后窗口比较）
                if (highPriceLast.compareTo(highPrice) == 1) {
                    highPrice = highPriceLast;
                }
                if (lowPriceLast.compareTo(lowPrice) == -1) {
                    lowPrice = lowPriceLast;
                }

                //11.计算分时成交量和成交金额
                tradeAmt = tradeAmtDay - tradeAmtDayLast;
                tradeVol = tradeVolDay - tradeVolDayLast;
            }

            //12.开盘价与高低价比较
            if (openPrice.compareTo(highPrice) == 1) {
                highPrice = openPrice;
            }
            if (openPrice.compareTo(lowPrice) == -1) {
                lowPrice = openPrice;
            }

            //13.封装结果数据
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

            out.collect(sectorBean);

            //14.缓存当前板块数据到状态中
            sectorMs.put(sectorCode, sectorBean);
        }
    }
}
