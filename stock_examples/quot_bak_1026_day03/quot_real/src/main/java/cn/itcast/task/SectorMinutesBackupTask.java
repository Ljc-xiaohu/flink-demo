package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.map.SectorPutHDFSMapFunction;
import cn.itcast.function.window.SectorWindowFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * 板块分时数据备份到HDFS
 */
public class SectorMinutesBackupTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.设置HDFS存储路径
         * 2.设置数据文件参数:大小、分区、格式、前缀、后缀
         * 3.数据分组
         * 4.划分个股时间窗口
         * 5.个股窗口数据处理
         * 6.划分板块时间窗口
         * 7.板块窗口数据处理
         * 8.转换并封装数据
         * 9.写入HDFS
         */
        //1.设置HDFS存储路径
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.SECTOR_SEC_HDFS_PATH);
        //2.设置数据文件参数:大小、分区、//前缀、后缀
        //大小
        bucketingSink.setBatchSize(16384L);
        //分区
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.HDFS_BUCKETER));//yyyyMMdd

        //3.数据分组
        waterData.keyBy(new KeyFunction())
                // 4.划分个股时间窗口
                .timeWindow(Time.minutes(1))
                //5.个股窗口数据处理
                .apply(new StockMinutesWindowFunction())
                // 6.划分板块时间窗口
                .timeWindowAll(Time.minutes(1))
                //.timeWindowAll(Time.minutes(240))
                //7.板块窗口数据处理
                .apply(new SectorWindowFunction())
                // 8.转换并封装数据
                .map(new SectorPutHDFSMapFunction())
                //9.写入HDFS
                .addSink(bucketingSink);
    }
}
