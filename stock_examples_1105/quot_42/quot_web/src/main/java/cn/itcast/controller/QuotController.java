package cn.itcast.controller;

import cn.itcast.bean.QuotResult;
import cn.itcast.service.QuotService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Web行情查询控制类
 */
@RestController
@RequestMapping("quot")
public class QuotController {

    @Autowired
    private QuotService quotService;

    @RequestMapping("test")
    public List<Map<String, Object>> test(){
        List<Map<String, Object>> list = quotService.query();
        return list;
    }

    /**
     * 1.国内指数
     */
   @RequestMapping("/index/all")
   public QuotResult indexQuery() throws SQLException{
       QuotResult result = quotService.indexQuery();
       return result;
   }

    /**
     * 2.板块指数
     */
    @RequestMapping("/sector/all")
    public QuotResult sectorQuery() throws SQLException {
        return quotService.sectorQuery();
    }

    /**
     * 3.涨跌幅行情查询
     */
    @RequestMapping("/stock/increase")
    public QuotResult incrQuery() throws SQLException {
        return quotService.incrQuery();
    }

    /**
     * 4.涨停跌停数
     */
    @RequestMapping("/stock/updown/count")
    public JSONObject updCnt() throws SQLException {
        return quotService.updCnt();
    }

    /**
     * 5.个股涨跌幅度
     */
    @RequestMapping("/stock/updown")
    public QuotResult updown() throws SQLException {
        return quotService.updown();
    }

    /**
     * 6.成交量对比
     */
    @RequestMapping("/stock/tradevol")
    public JSONObject tradeVolCompare() throws SQLException {
        return quotService.tradeVolCompare();
    }
    /**
     * 7.外盘指数查询
     */
    @RequestMapping("/external/index")
    public QuotResult exterIndex() {
        return quotService.exterIndex();
    }

    /**
     * 8.个股分时行情列表查询
     */
    @RequestMapping("/stock/all")
    public QuotResult stockAll() throws SQLException {
        return quotService.stockAll();
    }
    /**
     * 9.个股搜索/模糊查询
     */
    @RequestMapping("/stock/search")
    public QuotResult searchQuery(String searchStr) throws SQLException {
        return quotService.searchQuery(searchStr);
    }
    /**
     * 10.指定个股分时行情数据查询
     */
    @RequestMapping("/stock/screen/time-sharing")
    public QuotResult timeSharing(String code) throws SQLException {
        return quotService.timeSharing(code);
    }
    /**
     * 11.个股日K线查询
     */
    @RequestMapping("/stock/screen/dkline")
    public QuotResult stockDayKline(String code) {
        return quotService.stockDayKline(code);
    }
    /**
     * 12.个股最新秒级行情
     */
    @RequestMapping("/stock/screen/second")
    public QuotResult second(String code){
        return quotService.second(code);
    }
    /**
     * 13.个股最新分时行情/分时详情
     */
    @RequestMapping("/stock/screen/second/detail")
    public JSONObject detail(String code) throws SQLException {
        return quotService.detail(code);
    }
    /**
     * 14.个股描述/主营业务
     */
    @RequestMapping("/stock/describe")
    public JSONObject stockDesc(String code) {
        return quotService.stockDesc(code);
    }

}
