package cn.itcast.function.map;

import cn.itcast.avro.SseAvro;
import cn.itcast.bean.CleanBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Author itcast
 * Date 2020/10/26 14:53
 * Desc
 */
public class SseAvro2CleanBeanMapFunction  implements MapFunction<SseAvro, CleanBean> {
    @Override
    public CleanBean map(SseAvro sseAvro) throws Exception {
        CleanBean cleanBean = new CleanBean();
        cleanBean.setMdStreamId(sseAvro.getMdStreamID().toString());
        cleanBean.setSecCode(sseAvro.getSecurityID().toString());
        cleanBean.setSecName(sseAvro.getSymbol().toString());
        cleanBean.setTradeVolumn(sseAvro.getTradeVolume());
        cleanBean.setTradeAmt(sseAvro.getTotalValueTraded());
        cleanBean.setPreClosePx(BigDecimal.valueOf(sseAvro.getPreClosePx()));
        cleanBean.setOpenPrice(BigDecimal.valueOf(sseAvro.getOpenPrice()));
        cleanBean.setMaxPrice(BigDecimal.valueOf(sseAvro.getHighPrice()));
        cleanBean.setMinPrice(BigDecimal.valueOf(sseAvro.getLowPrice()));
        cleanBean.setTradePrice(BigDecimal.valueOf(sseAvro.getTradePrice()));
        cleanBean.setEventTime(sseAvro.getTimestamp());
        cleanBean.setSource("sse");
        return cleanBean;
    }
}
