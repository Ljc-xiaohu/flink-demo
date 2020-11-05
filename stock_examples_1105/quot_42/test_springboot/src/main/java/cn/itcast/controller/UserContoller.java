package cn.itcast.controller;

import cn.itcast.bean.User;
import cn.itcast.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/11/2 16:44
 * Desc
 */
@RestController//表示这是一个Controller控制类,用来接收客户端请求并返回json数据
@RequestMapping("user")//表示接收localhost:6666/user开头的请求
public class UserContoller {

    //依赖注入-就是Spring将我们需要的对象创建好并设置进来!
    @Autowired//表示自动从Spring中找到UserService类型的对象设置给userService变量
    private UserService userService;

    /**
     * 添加用户-注解sql方法
     */
    @RequestMapping("add")//表示接收localhost:6666/user/add开头的请求,并将参数封装到User参数中去
    public String add(User user){
        System.out.println("接收到了客户端请求,并将请求参数封装到了user对象中:" + user);
        System.out.println("接着调用service,将user传递个service做处理");
        userService.add(user);
        return "success";
    }

    /**
     * 添加用户-配置文件sql方法
     */
    @RequestMapping("add2")//表示接收localhost:6666/user/add开头的请求,并将参数封装到User参数中去
    public String add2(User user){
        System.out.println("add2接收到了客户端请求,并将请求参数封装到了user对象中:" + user);
        System.out.println("add2接着调用service,将user传递个service做处理");
        userService.add2(user);
        return "success";
    }

    /**
     * 根据id查询单个用户
     */
    //localhost:6666/user/findOneById
    @RequestMapping("findOneById")
    public User findOneById(Long id){
        User user = userService.findOneById(id);
        return user;
    }

    /**
     * 查询所有用户并封装为List<Bean>
     */
    //localhost:6666/user/findAll
    @RequestMapping("findAll")
    public List<User> findAll(){
        List<User> list = userService.findAll();
        return list;
    }
    /**
     * 查询所有用户并封装为List<Map<字段名,字段值>>>
     */
    //localhost:6666/user/findAllWithMap
    @RequestMapping("findAllWithMap")
    //List<Map<字段名,字段值>>
    public List<Map<String,Object>> findAllWithMap(){
        List<Map<String,Object>> list = userService.findAllWithMap();
        return list;
    }

}
