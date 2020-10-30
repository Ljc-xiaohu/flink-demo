package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.map.StockPutHDFSMapFunction;
import cn.itcast.function.window.StockMinutesWindowFunction;
import cn.itcast.standard.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * Author itcast
 * Date 2020/10/27 9:20
 * Desc 分时行情数据备份到HDFS核心业务类
 */
public class StockMinutesBackupTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //开发步骤：
        //1.创建BucketingSink并设置HDFS存储路径(会自动创建)
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.STOCK_SEC_HDFS_PATH);
        //2.设置数据文件参数:大小、分区、//前缀、后缀
        bucketingSink.setBatchSize(Long.parseLong(QuotConfig.HDFS_BATCH));
        bucketingSink.setBucketer(new DateTimeBucketer(QuotConfig.HDFS_BUCKETER));
        /*
        https://blog.csdn.net/kisimple/article/details/83998238
        in-progress，分桶下正在被写入的文件，一个分桶只会有一个。
        文件名格式为{in_progress_prefix}{part_prefix}-{parallel_task_index}-{count}{part_suffix}{in_progress_suffix}；
        pending，in-progress状态的文件关闭后进入pending状态，文件重命名，等待Checkpoint。
        文件名格式为{pending_prefix}{part_prefix}-{parallel_task_index}-{count}{part_suffix}{pending_suffix}；
        finished，Checkpoint成功之后，pending状态的文件即可置为finished状态，文件重命名，该状态的文件即为最终产生的可用文件
        */
        //前缀
        bucketingSink.setInProgressPrefix("stock_");
        bucketingSink.setPendingPrefix("stock2_");//挂起状态的前缀
        //后缀
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");

        //3.数据分组.keyBy(CleanBean::getSecCode)
        waterData.keyBy(CleanBean::getSecCode)
        //4.划分时间窗口.timeWindow(Time.seconds(60))
        .timeWindow(Time.seconds(60))
        //5.数据处理 .apply(new StockMinutesWindowFunction())
        .apply(new StockMinutesWindowFunction())
        //6.转换数据:设置分割符并进行数据拼接.map(new StockPutHDFSMapFunction())
        .map(new StockPutHDFSMapFunction())
        //7.写入HDFS.addSink(bucketingSink);
        .addSink(bucketingSink);

        /*
        测试:
        1.清空hdfs指定路径
        hadoop fs -rmr /quot_data
        2.启动StockStreamApplication
        3.写完之后去HDFS查看:
        http://node01:50070/explorer.html#/quot_data/dev/stock
         */

    }
}
