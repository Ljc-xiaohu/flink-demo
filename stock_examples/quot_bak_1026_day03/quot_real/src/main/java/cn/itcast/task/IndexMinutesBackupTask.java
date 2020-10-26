package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.map.IndexPutHDFSMapFunction;
import cn.itcast.function.window.IndexMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * 指数分时行情数据备份
 */
public class IndexMinutesBackupTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.设置HDFS存储路径
         * 2.设置数据文件参数:大小、分区、//前缀、后缀
         * 3.数据分组
         * 4.划分时间窗口
         * 5.数据处理
         * 6.转换并封装数据
         * 7.写入HDFS
         */
        // 1.设置HDFS存储路径
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.INDEX_SEC_HDFS_PATH);
        //2.设置数据文件参数:大小、分区、//前缀、后缀
        //大小
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.HDFS_BATCH));
        //分区
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.HDFS_BUCKETER));

        //3.数据分组
        waterData.keyBy(new KeyFunction())
                // 4.划分时间窗口
                .timeWindow(Time.minutes(1))
                //5.数据处理
                .apply(new IndexMinutesWindowFunction())
                //6.转换并封装数据
                .map(new IndexPutHDFSMapFunction())
                .addSink(bucketingSink);
        //写完之后去HDFS查看:
        // http://node01:50070/explorer.html#/quot_data/dev/index
        //先删除之前的:
        // hadoop fs -rmr /quot_data
    }
}
