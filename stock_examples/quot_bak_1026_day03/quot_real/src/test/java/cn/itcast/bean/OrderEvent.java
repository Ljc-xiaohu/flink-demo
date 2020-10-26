package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Date 2020/9/21
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    //  new OrderEvent(1, "create", 1558430842000L),//2019-05-21 17:27:22
    private int userId;
    private String status;
    private Long eventTime;
}
