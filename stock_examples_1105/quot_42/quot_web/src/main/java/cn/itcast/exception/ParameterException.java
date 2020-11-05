package cn.itcast.exception;

import cn.itcast.constant.BSCode;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 自定义异常，请求参数有误，
 * 遇到该错误需抛出终止请求，不可try catch
 */
@Data
@NoArgsConstructor
public class ParameterException extends CommException{
    /**
     * 错误码，由BSCode中获取
     * */
    private Integer errorCode;

    public ParameterException(String message){
        super(message);
    }
    public ParameterException(Integer errorCode, String message){
        super(message);
        this.errorCode=errorCode;
    }
    public ParameterException(BSCode bspCode){
        super(bspCode.getMsg());
        this.errorCode=bspCode.getSubCode();
    }
}
