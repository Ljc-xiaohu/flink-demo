package cn.itcast.exception;

import cn.itcast.constant.BSCode;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 自定义异常基类，程序中需要捕获处理的仅自定义异常，项目中自定义异常均继承该类
 */
@Data
@NoArgsConstructor
public class CommException extends RuntimeException {
    /**
     * 错误码，由BSCode中获取
     */
    private Integer errorCode = 500;

    public CommException(String message) {
        super(message);
    }

    public CommException(Integer errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    public CommException(BSCode bsCode){
        super(bsCode.getMsg());
        this.errorCode=bsCode.getSubCode();
    }
}
