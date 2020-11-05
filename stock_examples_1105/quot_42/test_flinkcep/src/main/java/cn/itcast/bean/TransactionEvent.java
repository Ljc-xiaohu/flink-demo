package cn.itcast.bean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * account 表是账户信息，amount 为转账金额，timeStamp 是交易时的时间戳信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionEvent {
    private String accout;
    private Double amount;
    private Long timeStamp;
}
