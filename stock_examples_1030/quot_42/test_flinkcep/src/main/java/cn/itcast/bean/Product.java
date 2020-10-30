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
public class Product {
    private Long goodsId; //商品ID
    private Double goodsPrice; //商品价格
    private String goodsName; //商品名称
    private String alias; //中文名称
    private Long orderTime;//事件时间
    private Boolean status;//价格阀值状态，ture：超过阀值，false:未超过阀值
}
