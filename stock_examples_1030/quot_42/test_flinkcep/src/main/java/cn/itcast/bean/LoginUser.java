package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Date 2020/9/19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginUser {
    //new LoginUser (1, "192.168.0.1", "fail", 1558430842000L),//2019-05-21 17:27:22
    private int userId;
    private String ip;
    private String status;
    private Long eventTime;
}
