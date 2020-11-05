package cn.itcast.cron;

import java.util.ArrayList;
import java.util.List;

/**
 * Author itcast
 * Date 2020/11/4 16:53
 * Desc
 */
public class TestDemo {
    public static void main(String[] args) {
        //Integer extends Number
        Number number1 = 1;
        Integer number2 = 2;
        number1 = number2;//父类类型接收子类对象!正确!

        List<Number> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();

        //list1 = list2;//正确吗? 错误! Java中带泛型的对象没有子父关系! 后面学习scala中有逆变协变非变可以!


        System.out.println(list1.getClass());//class java.util.ArrayList
        System.out.println(list2.getClass());//class java.util.ArrayList
        System.out.println(list1.getClass() == list2.getClass());//true or false?---true! //泛型只存在编译期,运行时泛型擦除了


    }
}
