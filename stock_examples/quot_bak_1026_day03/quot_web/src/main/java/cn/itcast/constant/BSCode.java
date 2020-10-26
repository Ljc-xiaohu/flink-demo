package cn.itcast.constant;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 大数据服务与应用返回的消息代码，包含错误、警告、成功
 */
@Data
@AllArgsConstructor
public class BSCode {
    /**
     * 成功状态，状态码固定为0
     * */
    public static final BSCode EXEC_SUCCESS = new BSCode(HttpCode.SUCC_200.getCode(), 0, "请求成功", "Query success");
    /**
     * 1000-1049 用户权限、凭证信息错误，状态码
     */
    public static final BSCode DRS_TOKEN_NULL = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1000,"Token为空", "Token is null");
    public static final BSCode DRS_TOKEN_ILLEGAL = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1001, "非法Token", "Illegal token");
    public static final BSCode DRS_TOKEN_EXPIRED = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1002, "Token已过期", "Token is expired");
    public static final BSCode DRS_AUTH_ERROR = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1003, "凭证(用户名/密码或Token)为空或错误", "Credential is null");
    public static final BSCode DRS_ROLE_EMPTY = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1004, "用户角色为空", "Role is empty");
    public static final BSCode DRS_ACCESS_DENY = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1005, "用户无权限不足", "Permission denied");
    public static final BSCode DIRECT_ACCESS_DENY = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1006, "不允许直接访问，请通过网关访问", "Direct access is not allowed");
    /**
     * 1050-1099 数据源错误信息 状态码
     */
    public static final BSCode DB_SQL_INJECT = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1050, "SQL存在风险或越权", "SQL unauthorized");
    public static final BSCode DB_SQL_ERROR = new BSCode(HttpCode.ERR_REQ_401.getCode(), 1051, "SQL语句错误", "SQL syntax error");
    public static final BSCode DB_DIC_ERROR = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1052, "数据字典查询错误", "DB dictionary error");
    public static final BSCode DB_QUERY_EXCEED = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1053, "查询数据量超过阈值，请添加限定条件！", "Query result over size");
    public static final BSCode DB_SQL_NO_SCHEMA = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1054, "SQL错误，表没有带schema", "Table without schema");
    public static final BSCode DB_DATA_UNSATISFIED = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1055, "目前数据不满足，需要手工确认", "Current data in Database is unsatisfied");
    public static final BSCode DB_SQL_NUMS_EXCEED = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1056, "查询SQL语句超过限制", "Query quantity oversize");
    /**
     * 1100-1149 服务内部错误状态码
     */
    public static final BSCode RESULT_OVERSIZE = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1100, "返回结果大小超出限制", "Query result over size");
    public static final BSCode MEMORY_OVERSIZE = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1101, "内存溢出", "Out of memory");
    public static final BSCode MEMORY_LEAK = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1102, "内存泄露", "Memory leak out");
    public static final BSCode REQUEST_PARAM_ERROR = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1103, "请求参数为空或错误", "Request param is null or error");

    /**
     * 1150-1199 请求参数校验错误， http code均为400
     * */
    public static final BSCode ILLEGAL_PARAMS = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1150, "请求参数包含非法字符", "Illegal request parameters");
    public static final BSCode ILLEGAL_SIZE = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1151, "请求参数集合大小非法，合法区间", "Illegal request collection size");
    public static final BSCode ILLEGAL_PARAMS_ISNULL = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1152, "请求参数不能为空", "Illegal request parameters is null");
    public static final BSCode UNEXIST_REDIS_COLUMN = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1153, "Redis请求操作列不存在", "Request column not in Redis");
    public static final BSCode ILLEGAL_PARAMS_DATE = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1154, "日期输入不正确（开始日期不能大于结束日期）", "Request parameters is not correct");
    public static final BSCode ILLEGAL_SELECT_RESULT = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1155, "查询SQL语句最外层结果集需指定查询列名", "The outermost query result of the SQL query statement needs to specify the query column name");
    public static final BSCode ILLEGAL_PARAMS_ERR = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1156, "请求参数不正确", "Request parameters is not correct");
    public static final BSCode ILLEGAL_PARAMS_DATE_LENGTH = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1157, "日期长度输入不正确", "Request parameters length is incorrect");
    public static final BSCode ILLEGAL_PARAMS_DATE_INCORRECT = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1158, "日期输入不正确", "Request parameters length is incorrect");
    /**
     * 1200-1249 请求参数校验错误， http code均为400
     * */
    public static final BSCode INVESTOR_USER = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1200, "传入数据异常", "invalid request params");
    public static final BSCode INVESTOR_CANCELLATION = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1201, "销户", "account cancellation");
    public static final BSCode INVESTOR_HBASE = new BSCode(HttpCode.ERR_REQ_400.getCode(), 1202, "Hbase环境异常", "hbase environmental anormaly");
    public static final BSCode INVESTOR_TASKFAIL = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1203, "跑批任务失败", "failure of running etl task");
    public static final BSCode INVESTOR_INVALID = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1204, "ETL跑批数据异常", "etl data exception");
    public static final BSCode INVESTOR_INNER = new BSCode(HttpCode.ERR_SERV_500.getCode(), 1205, "程序内部异常", "internal exception of program");
    
    
    /**
     * 消息主编码，即200,300,400等通用HTTP状态码
     * */
    private int code;
    /**
     * 消息子编码，即服务内具体的消息编码，5位
     * */
    private int subCode;
    /**
     * 返回消息，中文
     * */
    private String msg;
    /**
     * 返回消息，英文
     * */
    private String msgEN;
}