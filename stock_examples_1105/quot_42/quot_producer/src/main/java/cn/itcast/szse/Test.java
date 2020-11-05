package cn.itcast.szse;

import java.math.BigDecimal;

/**
 * Author itcast
 * Date 2020/10/23 15:33
 * Desc
 */
public class Test {
    public static void main(String[] args) {
        long tradeAmt = new BigDecimal("40577852.52").longValue();
        System.out.println(tradeAmt);
    }
}
