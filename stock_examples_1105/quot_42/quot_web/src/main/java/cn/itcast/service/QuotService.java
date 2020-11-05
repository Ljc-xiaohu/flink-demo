package cn.itcast.service;

import cn.itcast.bean.QuotResult;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface QuotService {
    /**
     * 1.国内指数
     */
    QuotResult indexQuery() throws SQLException;
    /**
     * 2.板块指数
     */
    QuotResult sectorQuery() throws SQLException;
    /**
     * 3.涨跌幅行情查询
     */
    QuotResult incrQuery() throws SQLException;
    /**
     * 4.涨停跌停数
     */
    JSONObject updCnt() throws SQLException;
    /**
     * 5.个股涨跌幅度
     */
    QuotResult updown() throws SQLException;
    /**
     * 6.成交量对比
     */
    JSONObject tradeVolCompare() throws SQLException;
    /**
     * 7.外盘指数查询
     */
    QuotResult exterIndex();

    /**
     * 8.个股分时行情列表查询
     */
    QuotResult stockAll() throws SQLException;
    /**
     * 9.个股搜索/模糊查询
     */
    QuotResult searchQuery(String searchStr) throws SQLException;

    /**
     * 10.指定个股分时行情数据查询
     */
    QuotResult timeSharing(String code) throws SQLException;
    /**
     * 11.个股日K线查询
     */
    QuotResult stockDayKline(String code);
    /**
     * 12.个股秒级行情
     */
    QuotResult second(String code);
    /**
     * 13.个股最新分时行情/分时详情
     */
    JSONObject detail(String code) throws SQLException;

    /**
     * 14.个股描述/主营业务
     */
    JSONObject stockDesc(String code);

    List<Map<String, Object>> query();
}
