package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.map.StockPutHDFSMapFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * 个股分时行情数据备份
 */
public class StockMinutesBackupTask implements ProcessDataInterface {
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
        //1.设置HDFS存储路径(会自动创建)
        //bucketingSink封装写入HDFS参数
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.STOCK_SEC_HDFS_PATH);
        //2.设置数据文件参数:大小、分区、//前缀、后缀
        //大小
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.HDFS_BATCH));
        //设定批次滚动时间间隔30分钟
        bucketingSink.setBatchRolloverInterval(30 * 60 * 1000L);
        //分区
        //bucketingSink.setBucketer(new DateTimeBucketer<>("yyyy/MM/dd"));
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.HDFS_BUCKETER));//yyyyMMdd
        /*//https://blog.csdn.net/kisimple/article/details/83998238
        //in-progress，分桶下正在被写入的文件，一个分桶只会有一个。文件名格式为{in_progress_prefix}{part_prefix}-{parallel_task_index}-{count}{part_suffix}{in_progress_suffix}；
        //pending，in-progress状态的文件关闭后进入pending状态，文件重命名，等待Checkpoint。文件名格式为{pending_prefix}{part_prefix}-{parallel_task_index}-{count}{part_suffix}{pending_suffix}；
        //finished，Checkpoint成功之后，pending状态的文件即可置为finished状态，文件重命名，该状态的文件即为最终产生的可用文件，文件名格式之前已经描述过了；
        //前缀
        bucketingSink.setInProgressPrefix("stock-");
        bucketingSink.setPendingPrefix("stockPending-");//挂起
        //后缀
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");*/
        //前缀
        bucketingSink.setInProgressPrefix("stock_");
        bucketingSink.setPendingPrefix("stock2_");//挂起
        //后缀
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");

        //3.数据分组
        waterData.keyBy(new KeyFunction())
                //4.划分时间窗口
                .timeWindow(Time.seconds(60))
                //5.数据处理
                .apply(new StockMinutesWindowFunction())
                //6.转换并封装数据,设置写入格式和分割字段
                .map(new StockPutHDFSMapFunction())
                //7.写入HDFS
                .addSink(bucketingSink);
        //写完之后去HDFS查看:
        // http://node01:50070/explorer.html#/quot_data/dev/stock
        //先删除之前的:
        // hadoop fs -rmr /quot_data
    }
}
