package cn.itcast.exception;

import cn.itcast.constant.BSCode;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 自定义异常，数据查询返回的数据量超出大数据应用设置的阈值，
 * 遇到该错误需抛出终止请求，不可try catch
 */
@Data
@NoArgsConstructor
public class ExceedMemoryException extends CommException {
    /**
     * 错误码，由BspCode中获取
     * */
    private Integer errorCode;
    /**
     * 当发生数据查询数据量超出限制时，告知调用方每个批次请求的行数
     * */
    private int perBatchSize;
    /**
     * 当发生数据查询数据量超出限制时，告知调用方批次数量
     * */
    private int batchQuantity;

    public ExceedMemoryException(String message){
        super(message);
    }
    public ExceedMemoryException(Integer errorCode, String message){
        super(message);
        this.errorCode=errorCode;
    }
    public ExceedMemoryException(Integer errorCode, int perBatchSize, int batchQuantity, String message){
        super(message);
        this.errorCode=errorCode;
        this.perBatchSize=perBatchSize;
        this.batchQuantity=batchQuantity;
    }
    public ExceedMemoryException(BSCode bspCode){
        super(bspCode.getMsg());
        this.errorCode=bspCode.getSubCode();
    }
}
