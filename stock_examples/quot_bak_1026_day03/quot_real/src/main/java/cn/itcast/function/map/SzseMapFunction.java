package cn.itcast.function.map;

import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * SzseAvro转为CleanBean
 */
public class SzseMapFunction implements MapFunction<SzseAvro, CleanBean> {
    @Override
    public CleanBean map(SzseAvro value) throws Exception {
        CleanBean cleanBean = new CleanBean(
                value.getMdStreamID().toString(),
                value.getSecurityID().toString(),
                value.getSymbol().toString(),
                value.getTradeVolume(),
                value.getTotalValueTraded(),
                BigDecimal.valueOf(value.getPreClosePx()),
                BigDecimal.valueOf(value.getOpenPrice()),
                BigDecimal.valueOf(value.getHighPrice()),
                BigDecimal.valueOf(value.getLowPrice()),
                BigDecimal.valueOf(value.getTradePrice()),
                value.getTimestamp(),
                "szse"
        );
        return cleanBean;
    }
}
