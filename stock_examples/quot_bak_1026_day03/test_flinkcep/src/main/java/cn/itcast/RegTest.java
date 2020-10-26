package cn.itcast;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Date 2020/9/19
 * 正则使用
 */
public class RegTest {
    public static void main(String[] args) {
        //判断字符串是否包含数字 \d
        String str = "dfdfdf";
        //定义规则
        Pattern compile = Pattern.compile("\\d");//匹配数字
        Matcher matcher = compile.matcher(str);
        boolean b = matcher.find();
        if(b == true){
            System.out.println("有数字");
        }else{
            System.out.println("没有数字");
        }
    }
}
