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
 * Date 2020/10/24 11:27
 * Desc
 */
public class SseQuotSource extends AbstractSource implements PollableSource, Configurable {
    //定义一些变量用来存储配置文件中的变量的value
    private String host;
    private Integer port;
    private String userName;
    private String password;
    private String ftpDirectory;
    private String fileName;
    private String localDirectory;
    //定义一些线程池初始化参数
    private Integer corePoolSize;
    private Integer maxPoolSize;
    private Integer keepAliveTime;
    private Integer capacity;
    //定义一个线程池,用来启动多个线程任务,采集FTP服务器文件
    private ThreadPoolExecutor threadPoolExecutor;


    //获取配置中的变量的value
    @Override
    public void configure(Context context) {
        //通过context访问配置文件中的value
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

        //初始化线程池-注意:线程池中的多个线程任务本来就是异步执行的!
        threadPoolExecutor = new ThreadPoolExecutor(
                corePoolSize,//活跃线程数
                maxPoolSize,//最大线程数
                keepAliveTime,//最大空闲时间
                TimeUnit.SECONDS,//时间单位
                new ArrayBlockingQueue<>(capacity) //任务等待队列,未被调度的线程任务,会在该队列中排队
        );

    }

    //处理方法-也就是要完成FTP数据采集/解析/封装/序列化
    @Override
    public Status process() throws EventDeliveryException {
        //1.严谨一点做下时间判断
        long time = new Date().getTime();//当前时间
        if (time > DateTimeUtil.openTime && time < DateTimeUtil.closeTime) {
            //目前是交易时间,进行数据采集
            //因为FTP服务器的数据变化很快,每1s就有一个新文件,所以这里用线程池执行多线程任务进行采集
            threadPoolExecutor.execute(new AsyncTask());
            try {
                Thread.sleep(1000);//每隔1s执行一个线程任务
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return Status.READY;//处理成功
        } else {
            return Status.BACKOFF;//没有处理
        }

    }

    /**
     * 异步线程任务-也就是要完成FTP数据采集/解析/封装/序列化
     */
    public class AsyncTask implements Runnable {
        @Override
        public void run() {
            //1.从ftpDirectory下载行情文件到Flume本地文件夹localDirectory
            boolean flag = download(ftpDirectory, localDirectory);
            if (flag) {
                System.out.println("文件下载成功。。。。");
                try {
                    //2.读取Flume本地文件localDirectory + "/" + fileName
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
                            //6.else,将每一行的字符串数组转换为SseAvro对象
                            SseAvro sseAvro = transfer(arr);
                            //7.SseAvro对象序列化为字节数组
                            byte[] bytes = mySerialize(sseAvro);
                            //8.发送数据到channel:
                            //getChannelProcessor().processEvent(EventBuilder.withBody(bytes));
                            getChannelProcessor().processEvent(EventBuilder.withBody(bytes));
                        }
                        i++;
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private byte[] mySerialize(SseAvro sseAvro) {
            //下面的代码表示
            // 先创建一个字节数组输出流对象
            // 再准备一个写标准对象
            // 再创建一个二进制编码器,准备把数据写到字节数组输出流对象之后
            // 再调用写标准对象的write方法,将data使用二进制编码器写到字节数组输出流对象中
            // 最后返回字节数组输出流对象中的二进制数据,也就是byte[]
            //1.创建字节数组输出流对象bos=new ByteArrayOutputStream()
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            //2创建SpecificDatumWriter对象/写标准对象
            SpecificDatumWriter<SseAvro> datumWriter = new SpecificDatumWriter<>(sseAvro.getSchema());
            //3.创建编码器EncoderFactory.get().directBinaryEncoder
            BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);//null表示不重新使用之前的编码器
            try {
                //4写出数据流对象datumWriter.write(data,binaryEncoder)
                datumWriter.write(sseAvro, binaryEncoder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //5返回流对象中的字节数组bos.toByteArray()
            return bos.toByteArray();
        }

    }

    private SseAvro transfer(String[] arr) {
        SseAvro sseAvro = new SseAvro();
        sseAvro.setMdStreamID(arr[0].trim());//行情数据类型
        sseAvro.setSecurityID(arr[1].trim());//产品代码
        sseAvro.setSymbol(arr[2].trim());//产品名称
        sseAvro.setTradeVolume(Long.parseLong(arr[3].trim()));//目前总成交量
        sseAvro.setTotalValueTraded(Long.parseLong(arr[4].trim()));//目前总成金额
        sseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));//前收盘价
        sseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));//开盘价
        sseAvro.setHighPrice(Double.valueOf(arr[7].trim()));//最高价
        sseAvro.setLowPrice(Double.valueOf(arr[8].trim()));//最低价
        sseAvro.setTradePrice(Double.valueOf(arr[9].trim()));//最新价
        sseAvro.setClosePx(Double.valueOf(arr[10].trim()));//收盘价,如果还没有闭市,当前最新价就是收盘价
        sseAvro.setTradingPhaseCode("T01");//实时阶段及标志
        sseAvro.setTimestamp(new Date().getTime());//时间戳
        return sseAvro;
    }

    /**
     * 使用FTP的API下载文件.直接当作工具类用
     * 从source/ftpDirectory下载行情文件到Flume本地文件夹target/localDirectory
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