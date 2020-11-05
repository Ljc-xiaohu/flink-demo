package cn.itcast.service.impl;

import cn.itcast.bean.User;
import cn.itcast.mapper.UserMapper;
import cn.itcast.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/11/2 16:49
 * Desc
 */
@Service//表示这是一个service,并交给Spring管理
public class UserServiceImpl implements UserService {
    @Autowired
    private UserMapper userMapper;

    @Override
    public void add(User user) {
        System.out.println("service接收到controller传递过来的user,可以做一些业务处理...并调用dao/mapper将数据保存到MySQL");
        userMapper.add(user);
    }

    @Override
    public void add2(User user) {
        System.out.println("add2service接收到controller传递过来的user,可以做一些业务处理...并调用dao/mapper将数据保存到MySQL");
        userMapper.add2(user);
    }

    @Override
    public User findOneById(Long id) {
        User user = userMapper.findOneById(id);
        return user;
    }

    @Override
    public List<User> findAll() {
        return userMapper.findAll();
    }

    @Override
    public List<Map<String, Object>> findAllWithMap() {
        return userMapper.findAllWithMap();
    }
}
