package cn.itcast.exception;

import cn.itcast.bean.QuotResult;
import cn.itcast.constant.HttpCode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * @Date 2020/9/24
 * 异常包装类
 */
@Component
@Qualifier("exceptionPacker")
public class ExceptionPacker {

    public QuotResult pack(Exception ex){
        QuotResult quotResult = new QuotResult();
        //判断异常类型
        if(ex instanceof ParameterException){
            quotResult.setCode(((ParameterException) ex).getErrorCode());
        }else if(ex instanceof UnAuthorizedException){
            quotResult.setCode(((UnAuthorizedException) ex).getErrorCode());
        }else{
            quotResult.setCode(HttpCode.ERR_SERV_500.getCode());
        }
        quotResult.setMessage(ex.getMessage());
        return quotResult;
    }
}
