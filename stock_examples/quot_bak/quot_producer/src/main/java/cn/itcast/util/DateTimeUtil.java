package cn.itcast.util;

import java.util.Calendar;
import java.util.Date;

public class DateTimeUtil {
    public static Long openTime;//开市时间
    public static Long closeTime;//闭市时间
    static {
        Calendar calendar = Calendar.getInstance();//新建Calendar对象
        //获取开市时间
        calendar.setTime(new Date());
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,1);
        calendar.set(Calendar.SECOND,0);
        openTime = calendar.getTime().getTime();//开市时间

        //获取闭市时间
        calendar.set(Calendar.HOUR_OF_DAY,23);//原本应该是9:30~15:00,这里设置为0~23是为了避免下午上课时通过不了判断
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,0);
        closeTime = calendar.getTime().getTime();//闭市时间
    }

    public static void main(String[] args) {
        System.out.println(openTime);
        System.out.println(closeTime);
    }
}
