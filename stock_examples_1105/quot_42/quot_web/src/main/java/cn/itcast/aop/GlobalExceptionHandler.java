package cn.itcast.aop;

import cn.itcast.bean.QuotResult;
import cn.itcast.exception.ExceptionPacker;
import cn.itcast.util.ClientIPUtils;
import cn.itcast.util.FlowidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * Author itcast
 * Date 2020/10/19 12:33
 * Desc 全局异常处理器
 */
@ControllerAdvice
public class GlobalExceptionHandler {
    Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * 统一异常处理
     */
    @Autowired
    ExceptionPacker exceptionPacker;

    // 1.新建异常统一处理方法
    @ExceptionHandler
    @ResponseBody
    public QuotResult handleException(HttpServletRequest request, Exception ex){
        /**
         *  开发步骤：
         *  1.新建异常统一处理方法
         *  2.封装日志消息体
         *  3.自定义异常处理类
         *  (1)内存溢出异常
         * (2)请求参数异常
         *  4.封装异常返回结果
         */
        // 2.封装日志消息体
        logInfo(
                FlowidUtil.getFlowid(),
                request.getMethod(),
                request.getRequestURL(),
                ClientIPUtils.getClientIp(request),
                null,
                ex.getMessage(), //异常日志
                null,
                null,
                null,
                "quotService"
        );

        return exceptionPacker.pack(ex);
    }

    private void logInfo(Object ... args) {
        logger.info("flowId[{}];requestMethod[{}];requestUrl[{}];clientIp[{}];requestParams[{}];result[{}];startTime[{}];endTime[{}];costTime[{}];serverFlag[{}]",args);
    }
}
