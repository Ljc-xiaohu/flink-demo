package cn.itcast;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Date 2020/9/22
 * @Description: 主类，是启动类，通过main方法启动
 */
@SpringBootApplication
public class QuotWebApplicatiion {
    public static void main(String[] args) {
        SpringApplication.run(QuotWebApplicatiion.class,args);
    }
}
