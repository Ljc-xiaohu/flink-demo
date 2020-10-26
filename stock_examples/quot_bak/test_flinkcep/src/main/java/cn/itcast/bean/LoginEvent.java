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
public class LoginEvent {
    private String id;
    private String ip;
    private String status;
    private int count;
}
