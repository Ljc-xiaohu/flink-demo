package cn.itcast.util;

import cn.itcast.constant.DateFormatConstant;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtil {

    /**
     * String类型日期转long型时间戳
     */
    public static Long  stringToLong(String time ,String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    /**
     * Long时间戳转指定格式Long型时间
     * 1603681382->yyyyMMdd->"20201026"->20201026L
     */
    public static Long longTimestamp2LongFormat(Long time , String format){
        FastDateFormat dateFormat = FastDateFormat.getInstance(format);
        String str = dateFormat.format(new Date(time));
        Long lTime = Long.valueOf(str);
        return lTime;
    }

    /**
     * Long时间戳转指定格式String型时间
     * 1603681382->yyyyMMdd->"20201026"
     */
    public static String longTimestamp2String(Long time , String format){
        FastDateFormat dateFormat = FastDateFormat.getInstance(format);
        String str = dateFormat.format(time);
        return str;
    }

    public static void main(String[] args) {
        Long time1 = stringToLong("2020-10-10", "yyyy-MM-dd");
        System.out.println(time1);

        Long time2 = longTimestamp2LongFormat(time1, "yyyyMMdd");
        System.out.println(time2);

        String time3 = longTimestamp2String(time1, DateFormatConstant.format_yyyy_mm_dd);
        System.out.println(time3);
    }
}
