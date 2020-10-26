package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 通用分组函数
 */
public class KeyFunction implements KeySelector<CleanBean,String> {
    @Override
    public String getKey(CleanBean value) throws Exception {
        return value.getSecCode();
    }
}
