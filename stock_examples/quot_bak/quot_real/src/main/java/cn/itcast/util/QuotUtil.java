package cn.itcast.util;

import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;

/**
 * 数据校验工具类：校验时间和数据
 */
public class QuotUtil {
    //校验时间
    public static boolean checkTime(Object obj) {
        if (obj instanceof SseAvro) {
            SseAvro sseAvro = (SseAvro) obj;
            return sseAvro.getTimestamp() > SpecialTimeUtil.openTime && sseAvro.getTimestamp() < SpecialTimeUtil.closeTime;
        } else {
            SzseAvro szseAvro = (SzseAvro) obj;
            return szseAvro.getTimestamp() > SpecialTimeUtil.openTime && szseAvro.getTimestamp() < SpecialTimeUtil.closeTime;
        }
    }

    //校验数据，数据是否为0 （高开低收）
    public static boolean checkData(Object obj) {
        if (obj instanceof SseAvro) {
            SseAvro sseAvro = (SseAvro) obj;
            return sseAvro.getHighPrice() != 0 && sseAvro.getOpenPrice() != 0 && sseAvro.getLowPrice() != 0 && sseAvro.getClosePx() != 0;
        } else {
            SzseAvro szseAvro = (SzseAvro) obj;
            return szseAvro.getHighPrice() != 0 && szseAvro.getOpenPrice() != 0 && szseAvro.getLowPrice() != 0 && szseAvro.getClosePx() != 0;
        }
    }

    //过滤个股
    public static boolean isStock(CleanBean value) {
        return value.getMdStreamId().equals("MD002") || value.getMdStreamId().equals("010");
    }

    //过滤指数
    public static boolean isIndex(CleanBean value) {
        return value.getMdStreamId().equals("MD001") || value.getMdStreamId().equals("900");
    }
}
