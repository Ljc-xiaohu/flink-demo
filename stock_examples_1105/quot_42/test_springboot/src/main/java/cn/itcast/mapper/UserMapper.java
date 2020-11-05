package cn.itcast.mapper;

import cn.itcast.bean.User;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/11/2 16:55
 * Desc
 */
@Mapper//表示该接口是一个MyBatis的mapper接口
@Repository//表示该接口是一个持久层接口,spring会在运行时动态生成该接口的实现了对象并交给spring管理
public interface UserMapper {
    @Insert("INSERT INTO `user` (`id`, `username`, `password`, `nickname`) VALUES (NULL, #{username}, #{password}, #{nickname})")
    void add(User user);

    void add2(User user);

    User findOneById(Long id);

    List<User> findAll();

    List<Map<String, Object>> findAllWithMap();
}
