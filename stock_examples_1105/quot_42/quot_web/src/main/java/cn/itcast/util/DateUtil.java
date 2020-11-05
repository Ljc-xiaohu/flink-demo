package cn.itcast.util;

import cn.itcast.constant.DateConstants;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 日期工具类
 */
public class DateUtil {
    /**
     * 秒级行情最新查询起止时间
     * 样例：startSecTime=20200804170700, endSecTime=20200804170759
     */
    public static Map<String, String> getCurSecTime() {
        SimpleDateFormat df = new SimpleDateFormat(DateConstants.MM);
        Date date = new Date();
        String minStr = df.format(date);

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, Integer.valueOf(minStr));
        cal.set(Calendar.SECOND, 0);
        Date time = cal.getTime();

        SimpleDateFormat df2 = new SimpleDateFormat(DateConstants.YYYYMMDDHHMMSS);
        String startMin = df2.format(time);

        cal.set(Calendar.SECOND, 59);
        time = cal.getTime();
        String endMin = df2.format(time);

        HashMap<String, String> map = new HashMap<>();
        map.put("startSecTime", startMin);
        map.put("endSecTime", endMin);
        return map;
    }

    /**
     * 秒级行情日期格式化
     */
    public static String formatSecTime(Long time){
        SimpleDateFormat df = new SimpleDateFormat(DateConstants.HHMMSS);
        String date = df.format(time);
        return date;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getCurSecTime());
    }

    /**
     * 获取当前日期字符串形式YYYYMMDD
     */
   /* public static String getCurDate() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.YYYYMMDD);
        String date = df.format(new Date());
        return date;
    }*/
    /**
     * 获取当前日期字符串形式YYYY_MM_DD
     */
   /* public static String getLineCurDate() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.YYYY_MM_DD);
        String date = df.format(new Date());
        return date;
    }*/
    /**
     * 将字符串日期解析为long类型并返回
     */
    /*public static long getCurDateMs(String date) {
        SimpleDateFormat df = new SimpleDateFormat(Constants.YYYYMMDDHHMMSS);
        long time = 0;
        try {
            time = df.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }*/

    /**
     * 将字符串日期解析为String类型并返回
     */
    /*public static String getCurDateMsTime(String date) {
        SimpleDateFormat df = new SimpleDateFormat(Constants.YYYYMMDDHHMMSS);
        String formatDate = null;
        try {
            Date parseDate = df.parse(date);
            formatDate = df.format(parseDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return formatDate;
    }*/

    /**
     * 获取系统时间
     */
    /*public static String getSysDateFromStringTime(String date) throws ParseException {
        SimpleDateFormat sf = new SimpleDateFormat(Constants.YYYYMMDDHHMMSS2);
        Date time = sf.parse(date);
        SimpleDateFormat sf2 = new SimpleDateFormat(Constants.YYYYMMDDHHMMSS);
        return sf2.format(time).trim();
    }*/

    /**
     * 获取HHMMSS
     */
    /*public static String getHHmmss(String date) {
        SimpleDateFormat df = new SimpleDateFormat(Constants.HHMMSS);
        String format = df.format(new Date(Long.valueOf(date)));
        return format;
    }*/

    /**
     * Long型日期格式化
     */
   /* public static String formatLongTime(Long time){
        SimpleDateFormat df = new SimpleDateFormat(Constants.YYYYMMDDHHMMSS2);
        String date = df.format(time);
        return date;
    }*/

    /**
     * 分时行情最新查询起止时间
     */
    /*public static Map<String, Long> getCurMinTime() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        System.out.println(cal.getTime());
        long startMin = cal.getTime().getTime();

        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        System.out.println(cal.getTime());
        long endMin = cal.getTime().getTime();

        HashMap<String, Long> map = new HashMap<>();
        map.put("startMinTime", startMin);
        map.put("endMinTime", endMin);
        return map;
    }*/
}
