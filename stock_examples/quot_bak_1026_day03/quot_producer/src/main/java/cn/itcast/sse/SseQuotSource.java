package cn.itcast.sse;

import cn.itcast.avro.SseAvro;
import cn.itcast.util.DateTimeUtil;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.io.*;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Desc 使用flume自定义source从FTP服务器采集实时行情文本文件到本地并发送到Kafka
 * http://flume.apache.org/releases/content/1.9.0/FlumeDeveloperGuide.html#source
 */
//1.自定义FlumeSource:class SseQuotSource extends AbstractSource implements PollableSource, Configurable{}
public class SseQuotSource extends AbstractSource implements PollableSource, Configurable {
    //2.定义成员变量
    private String host;
    private Integer port;
    private String userName;
    private String password;
    private String ftpDirectory;
    private String fileName;
    private String localDirectory;
    private Integer corePoolSize;
    private Integer maxPoolSize;
    private Integer keepAliveTime;
    private Integer capacity;

    private ThreadPoolExecutor threadPoolExecutor;

    //3.重写configure方法获取配置参数并初始化线程池对象
    @Override
    public void configure(Context context) {
        host = context.getString("host");
        port = context.getInteger("port");
        userName = context.getString("userName");
        password = context.getString("password");
        ftpDirectory = context.getString("ftpDirectory");
        fileName = context.getString("fileName");
        localDirectory = context.getString("localDirectory");
        corePoolSize = context.getInteger("corePoolSize");
        maxPoolSize = context.getInteger("maxPoolSize");
        keepAliveTime = context.getInteger("keepAliveTime");
        capacity = context.getInteger("capacity");

        threadPoolExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(capacity)
        );
    }

    //4.重写process方法处理接收到的数据
    @Override
    public Status process() throws EventDeliveryException {
        //4.1获取当前时间
        long time = new Date().getTime();
        //4.2判断是否是交易时间段9:30~15:00
        if (time > DateTimeUtil.openTime && time < DateTimeUtil.closeTime) {
            //4.3创建异步处理任务并放入线程池进行调度
            threadPoolExecutor.execute(new AsyncTask());
            try {
                //4.4睡眠1s
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //4.5 if返回结果Status.READY表示有数据
            return Status.READY; //有数据
        }
        //4.6else返回结果Status.BACKOFF表示无数据
        return Status.BACKOFF;//无数据
    }


    /**
     * 创建异步线程，执行业务逻辑
     */
    private class AsyncTask implements Runnable {
        @Override
        public void run() {
            //1.从ftpDirectory下载行情文件到Flume本地文件夹localDirectory
            boolean flag = download(ftpDirectory, localDirectory);
            if (flag){
                System.out.println("下载成功...");
            }
            //2.读取本地文件
            try {
                if (flag) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(localDirectory + "/" + fileName))));
                    String str;
                    int i = 0;
                    while ((str = br.readLine()) != null) {
                        //3.每一行数据按照\\|切割为字符串数据
                        String[] arr = str.split("\\|");
                        //4.if判断是否是第一行/文件头 i == 0 则判断市场状态
                        if (i == 0) {
                            //5.判断市场状态如果arr[8].trim().startsWith("E")为闭市则跳过该文件不处理
                            if (arr[8].trim().startsWith("E")) {
                                break;
                            }
                        } else {
                            //6. else,将每一行的字符串数组转换为SseAvro对象
                            SseAvro sseAvro = transfer(arr);
                            //7.SseAvro对象序列化为直接数组
                            byte[] bytes = serializer(sseAvro);
                            //8.发送数据到channel: getChannelProcessor().processEvent(EventBuilder.withBody(bytes));
                            getChannelProcessor().processEvent(EventBuilder.withBody(bytes));
                            //System.out.println("发送成功...");
                        }
                        i++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 数据序列化
         */
        private byte[] serializer(SseAvro sseAvro) {
            SpecificDatumWriter<Object> datumWriter = new SpecificDatumWriter<>(sseAvro.getSchema());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);
            try {
                datumWriter.write(sseAvro, binaryEncoder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return baos.toByteArray();
        }

        /**
         * 字符数组转换为SseAvro对象
         */
        private SseAvro transfer(String[] arr) {
            SseAvro sseAvro = new SseAvro();
            sseAvro.setMdStreamID(arr[0].trim());
            sseAvro.setSecurityID(arr[1].trim());
            sseAvro.setSymbol(arr[2].trim());
            sseAvro.setTradeVolume(Long.valueOf(arr[3].trim()));
            sseAvro.setTotalValueTraded(Long.valueOf(arr[4].trim()));
            sseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));
            sseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));
            sseAvro.setHighPrice(Double.valueOf(arr[7].trim()));
            sseAvro.setLowPrice(Double.valueOf(arr[8].trim()));
            sseAvro.setTradePrice(Double.valueOf(arr[9].trim()));
            sseAvro.setClosePx(Double.valueOf(arr[10].trim()));
            sseAvro.setTradingPhaseCode("T01");
            sseAvro.setTimestamp(new Date().getTime());
            return sseAvro;
        }

        /**
         * 从ftpDirectory下载行情文件到Flume本地文件夹localDirectory
         */
        private boolean download(String source, String target) {
            boolean flag = false;
            //1.创建FTPClient对象
            FTPClient ftpClient = new FTPClient();
            try {
                //2.初始化ftpClient
                //设置IP和Port
                ftpClient.connect(host, port);
                //设置登陆用户名和密码
                ftpClient.login(userName, password);
                //设置编码格式
                ftpClient.setControlEncoding("UTF-8");
                //3.获取返回码ftpClient.getReplyCode();
                int replyCode = ftpClient.getReplyCode();
                //4.判断是否连接成功FTPReply.isPositiveCompletion(replyCode)
                if (FTPReply.isPositiveCompletion(replyCode)) {
                    //5.切换工作目录ftpClient.changeWorkingDirectory(source);
                    ftpClient.changeWorkingDirectory(source);
                    //6.设置被动模式ftpClient.enterLocalPassiveMode();https://blog.csdn.net/qq_16038125/article/details/72851142
                    ftpClient.enterLocalPassiveMode();
                    //7.禁用服务端远程验证ftpClient.setRemoteVerificationEnabled(false);如果设置为true，会验证提交得ip与主机ip是否一致，ip不一致得时候，就报错
                    ftpClient.setRemoteVerificationEnabled(false);
                    //8.获取工作目录的文件信息列表
                    FTPFile[] ftpFiles = ftpClient.listFiles();
                    //9.遍历文件
                    for (FTPFile ftpFile : ftpFiles) {
                        //10.输出文件ftpClient.retrieveFile(ftpFile.getName(), new FileOutputStream(new File(target + "/" + fileName)));
                        ftpClient.retrieveFile(ftpFile.getName(), new FileOutputStream(new File(target + "/" + fileName)));
                    }
                }
                //11.退出ftpClient.logout();
                ftpClient.logout();
                //12.更改状态
                flag = true;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (ftpClient.isConnected()) {
                    try {
                        ftpClient.disconnect();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            //13.返回状态
            return flag;
        }
    }
}
