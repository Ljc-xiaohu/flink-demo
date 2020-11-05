package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import cn.itcast.constant.DateFormatConstant;
import org.apache.commons.lang3.StringUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * 获取开市和闭市时间的工具类
 */
public class SpecialTimeUtil {
    /**
     * 开发步骤：
     * 1.获取配置文件日期数据
     * 2.新建Calendar对象，设置日期
     * 3.设置开市时间
     * 4.获取开市时间
     * 5.设置闭市时间
     * 6.获取闭市时间
     */
    public static Long openTime;//开市时间
    public static Long closeTime;//闭市时间
    static {
        String date = QuotConfig.config.getProperty("date");
        String oTime = QuotConfig.config.getProperty("open.time");
        String cTime = QuotConfig.config.getProperty("close.time");
        //日期对象
        Calendar calendar = Calendar.getInstance();

        if(StringUtils.isEmpty(date)){
            calendar.setTime(new Date()); //设置当日时间
        }else{
            //非空,则设置过去时间--为了方便开发和测试设置得时间
            calendar.setTimeInMillis(DateUtil.stringToLong(date, DateFormatConstant.format_yyyymmdd));
        }

        //设置开市时间
        if(StringUtils.isEmpty(oTime)){
            calendar.set(Calendar.HOUR_OF_DAY,9);
            calendar.set(Calendar.MINUTE,30);
        }else{
            //非空,方便开发和测试
            String[] arr = oTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY,Integer.valueOf(arr[0]));
            calendar.set(Calendar.MINUTE,Integer.valueOf(arr[1]));
        }
        calendar.set(Calendar.SECOND,0);
        openTime = calendar.getTime().getTime();

        //设置闭市时间
        if(StringUtils.isEmpty(cTime)){
            calendar.set(Calendar.HOUR_OF_DAY,15);
            calendar.set(Calendar.MINUTE,0);
        }else{
            //非空,方便开发和测
            String[] arr = cTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY,Integer.valueOf(arr[0]));
            calendar.set(Calendar.MINUTE,Integer.valueOf(arr[1]));
        }
        closeTime = calendar.getTime().getTime();
    }

    /**
     * 测试
     */
    public static void main(String[] args) {
        System.out.println(DateUtil.longTimestamp2String(openTime,"yyyy-MM-dd HH:mm:ss"));
        System.out.println(DateUtil.longTimestamp2String(closeTime,"yyyy-MM-dd HH:mm:ss"));
    }
}
