package cn.itcast.function.map;

import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * Author itcast
 * Date 2020/10/26 14:53
 * Desc
 */
public class SzseAvro2CleanBeanMapFunction implements MapFunction<SzseAvro, CleanBean> {
    @Override
    public CleanBean map(SzseAvro szseAvro) throws Exception {
        CleanBean cleanBean = new CleanBean();
        cleanBean.setMdStreamId(szseAvro.getMdStreamID().toString());
        cleanBean.setSecCode(szseAvro.getSecurityID().toString());
        cleanBean.setSecName(szseAvro.getSymbol().toString());
        cleanBean.setTradeVolumn(szseAvro.getTradeVolume());
        cleanBean.setTradeAmt(szseAvro.getTotalValueTraded());
        cleanBean.setPreClosePx(BigDecimal.valueOf(szseAvro.getPreClosePx()));
        cleanBean.setOpenPrice(BigDecimal.valueOf(szseAvro.getOpenPrice()));
        cleanBean.setMaxPrice(BigDecimal.valueOf(szseAvro.getHighPrice()));
        cleanBean.setMinPrice(BigDecimal.valueOf(szseAvro.getLowPrice()));
        cleanBean.setTradePrice(BigDecimal.valueOf(szseAvro.getTradePrice()));
        cleanBean.setEventTime(szseAvro.getTimestamp());
        cleanBean.setSource("szse");
        return cleanBean;
    }
}
