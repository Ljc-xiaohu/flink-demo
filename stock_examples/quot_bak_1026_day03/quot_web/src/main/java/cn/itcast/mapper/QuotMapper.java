package cn.itcast.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * QuotMapper：DAO层，与数据库交互的
 */
@Mapper
@Component
public interface QuotMapper {

    /**
     * 外盘指数查询
     */
    List<Map<String, Object>> exIndexQuery();

    /**
     * 个股日K线查询
     */
    List<Map<String, Object>> stockDayKline(@Param("code") String code);

    /**
     * 个股描述/主营业务
     */
    Map<String, Object> stockDesc(@Param("code") String code);

    /**
     * 当前周K/月K数据查询
     */
    List<Map<String, Object>> klineQuery(@Param("tableName") String tableName, @Param("firstTxDate") String firstTxDate, @Param("lastTxDate") String lastTxDate);

    /**
     * 查询交易日历表
     */
    Map<String, Object> queryDate();

    /**
     * 更新周K/月K日期
     */
    void updateKline(@Param("tableName") String tableName, @Param("firstTxDate") String firstTxDate, @Param("lastTxDate") String lastTxDate);

    /**
     * 测试
     */
    List<Map<String, Object>> query();
}
