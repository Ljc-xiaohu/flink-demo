package cn.itcast.service;

import cn.itcast.bean.User;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/11/2 16:48
 * Desc
 */
public interface UserService {
    void add(User user);

    void add2(User user);

    User findOneById(Long id);

    List<User> findAll();

    List<Map<String, Object>> findAllWithMap();
}
