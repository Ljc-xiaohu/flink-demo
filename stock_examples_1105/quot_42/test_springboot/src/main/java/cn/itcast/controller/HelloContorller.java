package cn.itcast.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author itcast
 * Date 2020/11/2 16:10
 * Desc 入门案例-控制类-接收客户端请求并返回结果数据
 */
@RestController//=@Controller(标识这是一个Controller并交给Spring管理,由Spring帮我们创建该对象)+@ResponseBody(表示会将返回值转为json返回)
@RequestMapping("hello")//表示会接收localhost:6666/hello开头的请求
public class HelloContorller {
    //@RequestMapping(path="test",method = RequestMethod.POST)//表示会接收localhost:6666/hello/test开头的请求
    @RequestMapping("test")//表示会接收localhost:6666/hello/test开头的请求
    public String test(String name){
        System.out.println("接收到客户端发送的请求为name="+name);
        String result = "hello " + name;
        System.out.println("并处理请求给客户端响应结果:"+ result);
        return result;
    }
}
