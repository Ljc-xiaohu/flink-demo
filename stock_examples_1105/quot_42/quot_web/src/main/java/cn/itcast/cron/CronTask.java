package cn.itcast.cron;

import cn.itcast.mapper.QuotMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 定时任务：是将周K和月K上一个交易日的日期，更新成当日日期
 */
@EnableScheduling//开启定时任务
@Component //表示该对象交给Spring管理
public class CronTask {

    @Autowired
    QuotMapper quotMapper;

    //设置定时任务
    //定时任务表达式：0/10 * * * * ?    ：每十秒钟执行一次
    //https://cron.qqe2.com/
    @Scheduled(cron = "${cron.pattern.loader}")//
    public void cron(){
        System.out.println("每隔10s执行一次....");
        /**
         * 开发步骤：
         * 1.当前周K数据查询
         * 2.更新周K日期
         * 3.当前月K数据查询
         * 4.更新月K日期
         * 更新规则：
         * 有数据：将日期全部更新为最新日期
         * 无数据：不更新日期
         */

        //查询交易日历表
        Map<String,Object> map = quotMapper.queryDate();
        //SELECT trade_date,week_first_txdate,month_first_txdate FROM tcc_date WHERE trade_date = CURDATE()
        String tradeDate = map.get("trade_date").toString();//当前交易日期
        String weekFirstTxdate = map.get("week_first_txdate").toString();//这一周的第一个交易日
        String monthFirstTxdate = map.get("month_first_txdate").toString();//这一月的第一个交易日


        //1.当前周K数据查询
        List<Map<String,Object>> listWeek = quotMapper.klineQuery("bdp_quot_stock_kline_week","week_first_txdate","week_last_txdate");
        if(listWeek !=null && listWeek.size()>0){
            //2.更新周K日期--保证同一个sec_code的周K只有一条!
            quotMapper.updateKline("bdp_quot_stock_kline_week",weekFirstTxdate,tradeDate);
        }

        //3.当前月K数据查询
        List<Map<String,Object>> listMonth = quotMapper.klineQuery("bdp_quot_stock_kline_month","month_first_txdate","month_last_txdate");
        if(listMonth !=null && listMonth.size()>0){
            //4.更新月K日期
            quotMapper.updateKline("bdp_quot_stock_kline_month",monthFirstTxdate,tradeDate);
        }
    }
}
