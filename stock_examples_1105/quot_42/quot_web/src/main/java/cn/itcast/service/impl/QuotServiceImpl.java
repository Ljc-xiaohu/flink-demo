package cn.itcast.service.impl;

import cn.itcast.bean.QuotResult;
import cn.itcast.constant.HttpCode;
import cn.itcast.mapper.QuotMapper;
import cn.itcast.service.QuotService;
import cn.itcast.util.DateUtil;
import cn.itcast.util.DruidUtil;
import cn.itcast.util.HBaseUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Date 2020/9/22
 */
@Service
public class QuotServiceImpl implements QuotService {

    @Autowired
    private QuotMapper quotMapper;

    /**
     * 1.国内指数-Druid
     */
    @Override
    public QuotResult indexQuery() throws SQLException {
        System.out.println("执行了我们自己编写的代码!!!其他的和这里类似!!!");
/*
SELECT
indexCode as code,
indexName as name,
preClosePrice,
openPrice,
closePrice as tradePrice,
ROUND(CAST(closePrice as DOUBLE) - CAST(preClosePrice AS DOUBLE),2) AS undown,
ROUND((CAST(closePrice as DOUBLE) - CAST(preClosePrice AS DOUBLE) )/CAST(preClosePrice as DOUBLE),2) as increase,
tradeAmt,
tradeVol
FROM "index_stream_sse"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY
 */
        //1.获取连接
        Connection conn = DruidUtil.getConn();
        //2.获取st
        Statement st = conn.createStatement();
        //3.编写sql
        String sql = "SELECT\n" +
                "indexCode as code,\n" +
                "indexName as name,\n" +
                "preClosePrice,\n" +
                "openPrice,\n" +
                "closePrice as tradePrice,\n" +
                "ROUND(CAST(closePrice as DOUBLE) - CAST(preClosePrice AS DOUBLE),2) AS undown,\n" +
                "ROUND((CAST(closePrice as DOUBLE) - CAST(preClosePrice AS DOUBLE) )/CAST(preClosePrice as DOUBLE),2) as increase,\n" +
                "tradeAmt,\n" +
                "tradeVol\n" +
                "FROM \"index_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY ORDER BY __time desc limit 10";

        String sql2 = "SELECT\n" +
                "indexCode as code,\n" +
                "indexName as name,\n" +
                "preClosePrice,\n" +
                "openPrice,\n" +
                "closePrice as tradePrice,\n" +
                "ROUND(CAST(closePrice as DOUBLE) - CAST(preClosePrice AS DOUBLE),2) AS undown,\n" +
                "ROUND((CAST(closePrice as DOUBLE) - CAST(preClosePrice AS DOUBLE) )/CAST(preClosePrice as DOUBLE),2) as increase,\n" +
                "tradeAmt,\n" +
                "tradeVol\n" +
                "FROM \"index_stream_szse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY ORDER BY __time desc limit 10";

        //4.执行sql
        ResultSet rs = st.executeQuery(sql);
        //5.获取结果集并封装
        List<Map<String, Object>> list = new ArrayList<>();//准备集合用来存放所有行数据
        while (rs.next()){//处理每一行
            Map<String, Object> map = new HashMap<>();//准备集合用来存放其中一行数据
            map.put("code",rs.getObject(1));
            map.put("name",rs.getObject(2));
            map.put("preClosePrice",rs.getObject(3));
            map.put("openPrice",rs.getObject(4));
            map.put("tradePrice",rs.getObject(5));
            map.put("tradeAmt",rs.getObject(6));
            map.put("tradeVol",rs.getObject(7));
            list.add(map);
        }

        ResultSet rs2 = st.executeQuery(sql2);
        while (rs2.next()){//处理每一行
            Map<String, Object> map = new HashMap<>();//准备集合用来存放其中一行数据
            map.put("code",rs2.getObject(1));
            map.put("name",rs2.getObject(2));
            map.put("preClosePrice",rs2.getObject(3));
            map.put("openPrice",rs2.getObject(4));
            map.put("tradePrice",rs2.getObject(5));
            map.put("tradeAmt",rs2.getObject(6));
            map.put("tradeVol",rs2.getObject(7));
            list.add(map);
        }

        QuotResult result = new QuotResult();
        result.setItems(list);
        result.setCode(HttpCode.SUCC_200.getCode());

        DruidUtil.close(rs,null,null);
        DruidUtil.close(rs2,st,conn);
        return result;
    }

    /**
     * 2.板块指数-Druid
     */
    @Override
    public QuotResult sectorQuery() throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        String sql = "SELECT \n" +
                "sectorCode\n" +
                ",sectorName\n" +
                ",preClosePrice\n" +
                ",openPrice\n" +
                ",closePrice\n" +
                ",tradeAmtDay\n" +
                ",tradeVolDay\n" +
                "FROM \"sector_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "ORDER BY __time desc limit 10";
        //数据查询
        ResultSet rs = st.executeQuery(sql);
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("code", rs.getString(1));
            map.put("name", rs.getString(2));
            map.put("preClosePrice", rs.getString(3));
            map.put("openPrice", rs.getString(4));
            map.put("tradePrice", rs.getString(5));
            map.put("tradeAmt", rs.getString(6));
            map.put("tradeVol", rs.getString(7));
            list.add(map);
        }
        //关流
        DruidUtil.close(rs,st,conn);
        //封装返回结果
        QuotResult quotResult = new QuotResult();
        quotResult.setItems(list);
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        return quotResult;
    }

    /**
     * 3.涨跌幅行情查询-Druid
     */
    @Override
    public QuotResult incrQuery() throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        String sql = "SELECT \n" +
                "secCode\n" +
                ",secName\n" +
                ",increase\n" +
                ",tradePrice\n" +
                ",updown\n" +
                ",tradeVol\n" +
                ",amplitude\n" +
                ",preClosePrice\n" +
                ",tradeAmt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "ORDER BY __time desc limit 10";
        //数据查询
        ResultSet rs = st.executeQuery(sql);
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("code", rs.getString(1));
            map.put("name", rs.getString(2));
            map.put("increase", rs.getString(3));
            map.put("tradePrice", rs.getString(4));
            map.put("upDown", rs.getString(5));
            map.put("tradeVol", rs.getString(6));
            map.put("amplitude", rs.getString(7));
            map.put("preClosePrice", rs.getString(8));
            map.put("tradeAmt", rs.getString(9));
            list.add(map);
        }
        //关流
        DruidUtil.close(rs,st,conn);
        //封装数据
        QuotResult quotResult = new QuotResult();
        quotResult.setItems(list);
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        return quotResult;
    }

    /**
     * 4.涨停跌停数-Druid
     */
    @Override
    public JSONObject updCnt() throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        //涨停
        String upSql = "SELECT tradeTime,count(*) as cnt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "and CAST(increase as DOUBLE) > 0.07\n" +
                "GROUP BY 1";
        //跌停
        String downSql = "SELECT tradeTime,count(*) as cnt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "and CAST(increase as DOUBLE) < -0.07\n" +
                "GROUP BY 1";
        //涨停查询
        List<Map<String, Object>> upList = new ArrayList<>();
        ResultSet rs = st.executeQuery(upSql);
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("count", rs.getString(2));
            map.put("time", rs.getString(1));
            upList.add(map);
        }


        //跌停查询
        List<Map<String, Object>> downList = new ArrayList<>();
        rs = st.executeQuery(downSql);
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("count", rs.getString(2));
            map.put("time", rs.getString(1));
            downList.add(map);
        }
        //关流
        DruidUtil.close(rs,st,conn);
        //封装数据
        //JOSNObject可以理解为:Map<字段名,值>
        //JOSNObject可以理解为:Map<"upList",upList>
        //JOSNObject可以理解为:Map<"downList",downList>
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("upList",upList);
        jsonObject.put("downList",downList);
        return jsonObject;
    }
    /**
     * 5.个股涨跌幅度-Druid
     */
    @Override
    public QuotResult updown() throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        String sql = "SELECT \n" +
                "case when CAST(increase as DOUBLE) > 0.07 then '>7%'\n" +
                "when cast(increase as DOUBLE) > 0.03 and cast(increase as DOUBLE) <= 0.07 then '3~7%'\n" +
                "when cast(increase as DOUBLE) > 0 and cast(increase as DOUBLE) <= 0.03 then '0~3%'\n" +
                "when cast(increase as DOUBLE) > -0.03 and cast(increase as DOUBLE) <= 0 then '-3~0%'\n" +
                "when cast(increase as DOUBLE) > -0.07 and cast(increase as DOUBLE) <= -0.03 then '-7~-3%'\n" +
                "ELSE '<-7%' end as incr\n" +
                ",count(*) as cnt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "GROUP BY \n" +
                "case when CAST(increase as DOUBLE) > 0.07 then '>7%'\n" +
                "when cast(increase as DOUBLE) > 0.03 and cast(increase as DOUBLE) <= 0.07 then '3~7%'\n" +
                "when cast(increase as DOUBLE) > 0 and cast(increase as DOUBLE) <= 0.03 then '0~3%'\n" +
                "when cast(increase as DOUBLE) > -0.03 and cast(increase as DOUBLE) <= 0 then '-3~0%'\n" +
                "when cast(increase as DOUBLE) > -0.07 and cast(increase as DOUBLE) <= -0.03 then '-7~-3%'\n" +
                "ELSE '<-7%' end";

        //数据查询
        List<Map<String,Object>> list = new ArrayList<>();
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()){
            Map<String,Object> map = new HashMap<>();
            map.put("count",rs.getString(2));
            map.put("title",rs.getString(1));
            list.add(map);
        }
        //关流
        DruidUtil.close(rs,st,conn);
        //数据封装
        QuotResult quotResult = new QuotResult();
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        quotResult.setItems(list);
        return quotResult;
    }
    /**
     * 6.成交量对比-Druid
     */
    @Override
    public JSONObject tradeVolCompare() throws SQLException {
        /*//建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        //T日
        String sql = "SELECT TIME_FORMAT(\"__time\",'HH:mm') as tradeTime,SUM(PARSE_LONG(tradeVolDay)) as volDay\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY\n" +
                "group by 1 ";
        //T-1日
        String sqlYes = "SELECT TIME_FORMAT(\"__time\",'HH:mm') as tradeTime,SUM(PARSE_LONG(tradeVolDay)) as volDay\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY and \"__time\" < CURRENT_TIMESTAMP - INTERVAL '1' DAY\n" +
                "group by 1 \n";

      //查询T日
        ResultSet rs = st.executeQuery(sql);
        List<Map<String,Object>> list= new ArrayList<>();
        while (rs.next()){
            Map<String,Object> map= new HashMap<>();
            map.put("count",rs.getString(2));
            map.put("time",rs.getString(1));
            list.add(map);
        }

        //查询T-1日
        rs = st.executeQuery(sqlYes);
        List<Map<String,Object>> listYesterday= new ArrayList<>();
        while (rs.next()){
            Map<String,Object> map= new HashMap<>();
            map.put("count",rs.getString(2));
            map.put("time",rs.getString(1));
            listYesterday.add(map);
        }
        //关流
        DbUtil.close(rs,st,conn);
        //封装结果数据
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("volList",list);
        jsonObject.put("yesVolList",listYesterday);
        jsonObject.put("code",HttpCode.SUCC_200.getCode());

        return jsonObject;*/


        /**
         * 造数
         */
        JSONObject jsonObject = new JSONObject();
        List<JSONObject> list = new ArrayList<>();
        List<JSONObject> yesList = new ArrayList<>();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 9);
        cal.set(Calendar.MINUTE, 30);
        long timeInMillis = cal.getTimeInMillis();
        for (int i = 0; i < 100; i++) {
            long timeMills = timeInMillis + 60000L * i;
            String formatDate = simpleDateFormat.format(new Date(timeMills));
            JSONObject json = new JSONObject();
            JSONObject yesJson = new JSONObject();
            json.put("time",formatDate);
            yesJson.put("time",formatDate);
            json.put("count",i*10+10);
            yesJson.put("count",i*10+200);
            if(i>50){
                json.put("count",i*10+400);
                yesJson.put("count",i*10+20);
            }
            list.add(json);
            yesList.add(yesJson);
        }
        jsonObject.put("volList",list);
        jsonObject.put("yesVolList",yesList);
        return jsonObject;
    }


    /**
     * 7.外盘指数查询-MySQL
     */
    @Override
    public QuotResult exterIndex() {
        List<Map<String, Object>> list = quotMapper.exIndexQuery();
        //封装数据
        QuotResult quotResult = new QuotResult();
        quotResult.setItems(list);
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        return quotResult;
    }


    /**
     * 8.个股分时行情列表查询-Druid
     */
    @Override
    public QuotResult stockAll() throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        String sql = "SELECT \n" +
                "secCode\n" +
                ",secName\n" +
                ",preClosePrice\n" +
                ",openPrice\n" +
                ",closePrice\n" +
                ",highPrice\n" +
                ",lowPrice\n" +
                ",tradeAmt\n" +
                ",tradeVol\n" +
                ",tradeVolDay\n" +
                ",tradeAmtDay\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "ORDER BY __time desc limit 100";
        //执行查询
        ResultSet rs = st.executeQuery(sql);
        List<Map<String,Object>> list = new ArrayList<>();
        while (rs.next()){
            Map<String,Object> map = new HashMap<>();
            map.put("code",rs.getString(1));
            map.put("name",rs.getString(2));
            map.put("preClosePrice",rs.getString(3));
            map.put("openPrice",rs.getString(4));
            map.put("tradePrice",rs.getString(5));
            map.put("highPrice",rs.getString(6));
            map.put("lowPrice",rs.getString(7));
            map.put("tradeAmt",rs.getString(8));
            map.put("tradeVol",rs.getString(9));
            map.put("tradeVolDay",rs.getString(10));
            map.put("tradeAmtDay",rs.getString(11));
            list.add(map);
        }
        //关流
        DruidUtil.close(rs,st,conn);
        //封装结果
        QuotResult quotResult = new QuotResult();
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        quotResult.setItems(list);
        return quotResult;
    }

    /**
     * 9.个股搜索/模糊查询-Druid
     */
    @Override
    public QuotResult searchQuery(String searchStr) throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        String sql = "SELECT secCode,secName\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "and secCode like '%"+searchStr+"%'\n" +
                "limit 10";
        ResultSet rs = st.executeQuery(sql);
        List<Map<String,Object>> list = new ArrayList<>();
        while (rs.next()){
            Map<String,Object> map = new HashMap<>();
            map.put("code",rs.getString(1));
            map.put("name",rs.getString(2));
            list.add(map);
        }

        //封装结果数据
        QuotResult quotResult = new QuotResult();
        quotResult.setItems(list);
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        return quotResult;
    }

    /**
     * 10.指定个股分时行情数据查询-Druid
     */
    @Override
    public QuotResult timeSharing(String code) throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();

        String sql = "SELECT \n" +
                      "secCode\n" +
                      ",secName\n" +
                      ",preClosePrice\n" +
                      ",openPrice\n" +
                      ",closePrice\n" +
                      ",highPrice\n" +
                      ",lowPrice\n" +
                      ",tradeAmt\n" +
                      ",tradeVol\n" +
                      ",tradeVolDay\n" +
                      ",tradeAmtDay\n" +
                      ",tradeTime\n" +
                      "FROM \"stock_stream_sse\"\n" +
                      "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                      "and secCode = '"+code+"' and PARSE_LONG(tradeVol) >0";
        //数据查询
        ResultSet rs = st.executeQuery(sql);
        List<Map<String,Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("code", rs.getString(1));
            map.put("name", rs.getString(2));
            map.put("preClosePrice", rs.getString(3));
            map.put("openPrice", rs.getString(4));
            map.put("tradePrice", rs.getString(5));
            map.put("highPrice", rs.getString(6));
            map.put("lowPrice", rs.getString(7));
            map.put("tradeAmt", rs.getString(8));
            map.put("tradeVol", rs.getString(9));
            map.put("tradeVolDay", rs.getString(10));
            map.put("tradeAmtDay", rs.getString(11));
            map.put("date", rs.getString(12));
            list.add(map);
        }

        //封装结果数据
        QuotResult quotResult = new QuotResult();
        quotResult.setItems(list);
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        return quotResult;
    }


    /**
     * 11.个股日K线查询-MySQL
     */
    @Override
    public QuotResult stockDayKline(String code) {
        List<Map<String,Object>> list = quotMapper.stockDayKline(code);
        //封装结果
        QuotResult quotResult = new QuotResult();
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        quotResult.setItems(list);
        return quotResult;
    }
    /**
     * 12.个股最新秒级行情-HBase
     */
    @Override
    public QuotResult second(String code) {
        //5s * 15 = 75
        //获取当前分钟的起止时间
        Map<String, String> curSecTimeMap = DateUtil.getCurSecTime();
        String startSecTime = curSecTimeMap.get("startSecTime");
        String endSecTime = curSecTimeMap.get("endSecTime");
        //起止rowkey//测试
        String startKey = code+"20201026085454";//code+startSecTime;
        String endKey = code+"20201026085604~";//~表示包括//code+endSecTime;

        //查询hbase
        List<String> list = HBaseUtil.queryScan("quot_stock", "info", "data", startKey, endKey);
        List<Map<String,Object>> listMap = new ArrayList<>();
        for (String str : list) {
            Map<String,Object> map = new HashMap<>();
            JSONObject json = JSON.parseObject(str);
            map.put("date",DateUtil.formatSecTime(json.getLong("eventTime")));
            map.put("tradePrice",json.get("closePrice"));
            map.put("tradeVol",json.get("tradeVolDay"));
            map.put("tradeAmt",json.get("tradeAmtDay"));
            listMap.add(map);
        }
        //封装返回结果
        QuotResult quotResult = new QuotResult();
        quotResult.setItems(listMap);
        quotResult.setCode(HttpCode.SUCC_200.getCode());
        return quotResult;
    }

    /**
     * 13.个股最新分时行情(分时详情)-Druid
     */
    @Override
    public JSONObject detail(String code) throws SQLException {
        //建立JDBC
        Connection conn = DruidUtil.getConn();
        Statement st = conn.createStatement();
        String sql = "SELECT \n" +
                "preClosePrice\n" +
                ",openPrice\n" +
                ",closePrice\n" +
                ",highPrice\n" +
                ",lowPrice\n" +
                ",tradeAmtDay\n" +
                ",tradeVolDay\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '20' DAY\n" +
                "and  secCode = '"+code+"'\n" +
                "ORDER BY __time desc limit 1";

        //数据查询
        JSONObject jsonObject = new JSONObject();
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()){
            jsonObject.put("preClosePrice",rs.getString(1));
            jsonObject.put("openPrice",rs.getString(2));
            jsonObject.put("tradePrice",rs.getString(3));
            jsonObject.put("highPrice",rs.getString(4));
            jsonObject.put("lowPrice",rs.getString(5));
            jsonObject.put("tradeAmt",rs.getString(6));
            jsonObject.put("tradeVol",rs.getString(7));
        }

        return jsonObject;
    }

    /**
     * 14.个股描述/主营业务-MySQL
     */
    @Override
    public JSONObject stockDesc(String code) {
        Map<String,Object> map = quotMapper.stockDesc(code);
        //map转json
        JSONObject json = (JSONObject) JSON.toJSON(map);
        return json;
    }

    @Override
    public List<Map<String, Object>> query() {
        List<Map<String, Object>> list = quotMapper.query();
        return list;
    }
}
