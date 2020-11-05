package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.map.SectorPutHDFSMapFunction;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * Author itcast
 * Date 2020/10/27 17:30
 * Desc
 * 板块分时行情数据备份
 */
public class SectorMinutesBackupTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //1.设置HDFS存储路径
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.SECTOR_SEC_HDFS_PATH);
        //2.设置数据文件参数:大小、分区、格式、//前缀、后缀--可以不设置会有默认值
        bucketingSink.setBatchSize(16384L);//注意:板块数据较少达不到数据flush到文件的阈值,所以把文件大小设置小一点,强制flush数据到文件
        //bucketingSink.setBatchSize(Long.parseLong(QuotConfig.HDFS_BATCH));
        bucketingSink.setBucketer(new DateTimeBucketer(QuotConfig.HDFS_BUCKETER));

        //3.数据分组.keyBy(CleanBean::getSecCode)
        waterData.keyBy(CleanBean::getSecCode)
        //4.划分个股时间窗口.timeWindow(Time.minutes(1))
        .timeWindow(Time.minutes(1))
        //5.个股窗口数据处理.apply(new StockMinutesWindowFunction())
        .apply(new StockMinutesWindowFunction())
        //6.划分板块时间窗口.timeWindowAll(Time.minutes(1))
        .timeWindowAll(Time.minutes(1))
        //7.板块窗口数据处理.apply(new SectorWindowFunction())
        .apply(new SectorWindowFunction())
        //8.转换并封装数据为拼接的字符串
        .map(new SectorPutHDFSMapFunction())
        //9.写入HDFS.addSink(bucketingSink);
        .addSink(bucketingSink);
    }
}
