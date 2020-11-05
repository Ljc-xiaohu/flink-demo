package cn.itcast;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Author itcast
 * Date 2020/11/2 16:03
 * Desc
 */
@SpringBootApplication//表示这是一个SpringBoot入口类/应用程序
public class WebApplication {
    public static void main(String[] args) {
        //启动SpringBoot应用程序
        //注意:
        // 启动后会加载application.properties配置文件
        // 并扫描WebApplication同级包及其子包下的Spring相关注解
        SpringApplication.run(WebApplication.class,args);
    }
}
