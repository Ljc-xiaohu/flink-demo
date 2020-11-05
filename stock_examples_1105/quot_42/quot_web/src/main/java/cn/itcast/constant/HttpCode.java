package cn.itcast.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * 通用的Http错误码
 */
@AllArgsConstructor
public enum HttpCode {

    TEMP_100(100, "继续"),
    TEMP_101(101, "切换协议"),

    SUCC_200(200, "成功"),
    SUCC_201(201, "已创建"),
    SUCC_202(202, "已接受"),
    SUCC_203(203, "非授权信息"),
    SUCC_204(204, "无内容"),
    SUCC_205(205, "重置内容"),
    SUCC_206(206, "部分内容"),

    REDIR_300(300, "多种选择"),
    REDIR_301(301, "永久移动"),
    REDIR_302(302, "暂时移动"),
    REDIR_303(303, "查看其他位置"),
    REDIR_304(304, "未修改"),
    REDIR_305(305, "使用代理"),
    REDIR_307(307, "暂时重定向"),

    ERR_REQ_400(400, "错误请求"),
    ERR_REQ_401(401, "未授权"),
    ERR_REQ_403(403, "禁止"),
    ERR_REQ_404(404, "未找到"),
    ERR_REQ_405(405, "禁用的方法"),
    ERR_REQ_406(406, "不可接受"),
    ERR_REQ_407(407, "需要代理授权"),
    ERR_REQ_408(408, "请求超时"),
    ERR_REQ_409(409, "冲突"),
    ERR_REQ_410(410, "已删除"),
    ERR_REQ_411(411, "需要有效长度"),
    ERR_REQ_412(412, "未满足前提条件"),
    ERR_REQ_413(413, "请求实体过大"),
    ERR_REQ_414(414, "请求的URI过长"),
    ERR_REQ_415(415, "不支持的媒体类型"),
    ERR_REQ_416(416, "请求范围不符合要求"),
    ERR_REQ_417(417, "未满足期望要求"),

    ERR_SERV_500(500, "服务器内部错误"),
    ERR_SERV_501(501, "尚未实施"),
    ERR_SERV_502(502, "错误网关"),
    ERR_SERV_503(503, "服务不可用"),
    ERR_SERV_504(504, "网关超时"),
    ERR_SERV_505(505, "HTTP版本不受支持");

    @Getter @Setter private int code;
    @Getter @Setter private String info;
}