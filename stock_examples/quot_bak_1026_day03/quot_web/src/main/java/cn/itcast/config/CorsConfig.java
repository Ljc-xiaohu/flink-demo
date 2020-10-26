package cn.itcast.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * 允许跨域请求配置类对象
 */
@Configuration
public class CorsConfig extends WebMvcConfigurerAdapter {
    /**
     * 允许跨域访问
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        //* ：表示所有
        registry
                //配置可以被跨域的路径，可以任意配置，可以具体到直接请求路径。
                .addMapping("/**")
                //允许所有的请求header访问，可以自定义设置任意请求头信息，如："X-YAUTH-TOKEN"
                .allowedHeaders("*")
                ////允许所有的请求方法访问该跨域资源服务器，如：POST、GET、PUT、DELETE等。
                //.allowedMethods("POST", "GET", "PUT", "OPTIONS", "DELETE")
                .allowedMethods("*")
                //允许所有的请求域名访问我们的跨域资源，可以固定单条或者多条内容，如："http://cn.itcast.com"，只有此url可以访问我们的跨域资源。
                .allowedOrigins("*");

    }
}
