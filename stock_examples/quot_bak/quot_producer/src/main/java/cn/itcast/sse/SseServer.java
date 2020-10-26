package cn.itcast.sse;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 沪市行情服务端
 * 负责广播沪市实时行情数
 */
public class SseServer {
    //读取sseQuot.txt计算浮动成交量和成交金额后写入到sse.txt然后上传到ftp
    private static String inPath = "/export/servers/tmp/socket/sseQuot.txt";
    private static String outPath = "/export/servers/tmp/socket/sse.txt";
    private static String ftpPath = "/home/ftptest";

    //随机浮动成交量
    private static int[] volumn = new int[]{50, 80, 110, 140, 170, 200, 230, 260, 290, 320, 350, 380, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300};
    //随机浮动成交价格百分比
    private static Double[] price = new Double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95, -0.1, -0.2, -0.3, -0.4, -0.5, -0.6, -0.7, -0.8, -0.9, -0.15, -0.25, -0.35, -0.45, -0.55, -0.65, -0.75, -0.85, -0.95};

    private static Map<String, Map<String, Long>> map = new HashMap<>();

    public static void main(String[] args) throws IOException {
        if(SystemUtils.IS_OS_WINDOWS){
            inPath = "D:\\export\\servers\\tmp\\socket\\sseQuot.txt";
            outPath = "D:\\export\\servers\\tmp\\socket\\sse.txt";
        }

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");

        //解析文件缓存成交量和成交金额
        map = parseLocalFileToMap();

        //建立FTP连接
        FTPClient ftpClient = new FTPClient();
        ftpClient.setControlEncoding("UTF-8");
        ftpClient.connect("192.168.52.100", 21);
        //用户名和密码
        ftpClient.login("ftptest", "ftptest");
        int replyCode = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(replyCode)) {
            System.out.println("connect failed ftp server ...");
        }
        ftpClient.setControlEncoding("UTF-8");
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setConnectTimeout(6000);
        //FTP存放文件路径
        ftpClient.changeWorkingDirectory(ftpPath);

        //循环
        for (int i = 0; i < 3600; i++) {
            //将行情数据写入文件
            writerFile(outPath);
            try {
                FileInputStream inputStream = new FileInputStream(new File(outPath));
                //ftp写入文件名称
                ftpClient.storeFile("sse.txt", inputStream);
                inputStream.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                System.out.println("sleep pre ...:" + sf.format(new Date()));
                //1秒钟发送一次
                Thread.sleep(1000);
                System.out.println("after pre ...:" + sf.format(new Date()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //退出FTP
        ftpClient.logout();
    }

    /**
     * 将行情数据写入文件
     */
    public static void writerFile(String path) {
        try {
            FileOutputStream os = new FileOutputStream(path);
            OutputStreamWriter writer = new OutputStreamWriter(os, "UTF-8");
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            Random random = new Random();
            int priceIndex = random.nextInt(price.length);
            Double priceRandom = price[priceIndex];
            int volumnIndex = random.nextInt(volumn.length);
            int volumnRandom = volumn[volumnIndex];

            List<String> list = parseFile();
            StringBuffer sb = new StringBuffer();
            writer.write("HEADER|MTP1.00 |   1258021|" + list.size() + "|        |XSHG01|20181218-14:36:40.230|0|T1111" + "\n");
            for (int k = 1; k < list.size() - 1; k++) {
                String format = sf.format(new Date());
                String timeStamp = format.substring(11, 23);
                String line = list.get(k);
                if (line.contains("MD001") || line.contains("MD002")) {
                    String[] arr = line.split("\\|");
                    /*
                     *计算最新价、成交量和成交金额
                     */
                    //最新价
                    BigDecimal tradePriceBase = new BigDecimal(arr[9].trim());
                    BigDecimal tradePrice = tradePriceBase.multiply(new BigDecimal(1 + priceRandom));
                    tradePrice = tradePrice.setScale(2, RoundingMode.HALF_UP);

                    //取成交量和成交金额
                    Map<String, Long> volAmtMap = map.get(arr[1].trim());

                    Long tradeVol = volAmtMap.get("tradeVol");
                    Long tradeAmt = volAmtMap.get("tradeAmt");

                    //总成交量
                    Long tradeVolNew = 0l;
                    if (tradeVol != 0) {
                        tradeVolNew = tradeVol + volumnRandom;
                    }
                    //总成交金额
                    BigDecimal amt = tradePrice.multiply(new BigDecimal(volumnRandom)).setScale(2, RoundingMode.HALF_UP);
                    Long tradeAmtNew = tradeAmt + amt.longValue();

                    //返回总金额和总数量
                    volAmtMap.put("tradeVol", tradeVolNew);
                    volAmtMap.put("tradeAmt", tradeAmtNew);
                    map.put(arr[1].trim(), volAmtMap);

                    //计算最高价
                    BigDecimal highPrice = new BigDecimal(arr[7].trim());
                    //最高价和最新价比较
                    if (tradePrice.compareTo(highPrice) == 1) {
                        highPrice = tradePrice;
                    }

                    //计算最低价
                    BigDecimal lowPrice = new BigDecimal(arr[8].trim());
                    //最低价和最新价比较
                    if (tradePrice.compareTo(lowPrice) == -1) {
                        lowPrice = tradePrice;
                    }
                    //最低价和开盘价比较
                    BigDecimal openPrice = new BigDecimal(arr[6].trim());
                    if (openPrice.compareTo(lowPrice) == -1) {
                        lowPrice = openPrice;
                    }

                    if (line.contains("MD001") || line.contains("MD002")) { //指数
                        sb.append(arr[0]).append("|")
                                .append(arr[1]).append("|")
                                .append(arr[2]).append("|")
                                .append(tradeVolNew).append("|")
                                .append(tradeAmtNew).append("|")
                                .append(arr[5]).append("|")
                                .append(arr[6]).append("|")
                                .append(highPrice).append("|")
                                .append(lowPrice).append("|")
                                .append(tradePrice).append("|")
                                .append(tradePrice).append("|")
                                .append(arr[11]).append("|")
                                .append(timeStamp);

                        writer.write(sb.toString() + "\n");
                    }
                    sb.setLength(0);
                }
            }

            writer.close();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *解析sseQuot行情文件
     */
    public static List<String> parseFile() {
        ArrayList<String> list = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inPath)),"UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                list.add(lineTxt);
            }
            br.close();
        } catch (Exception e) {
            System.err.println("errors :" + e);
        }
        return list;
    }

    /**
     * 解析文件缓存成交量和成交金额
     */
    private static Map<String, Map<String, Long>> parseLocalFileToMap() {
        try {
            BufferedReader brSzseStock = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inPath)), "UTF-8"));
            String lineTxtStock = null;
            while ((lineTxtStock = brSzseStock.readLine()) != null) {
                String[] arr = lineTxtStock.split("\\|");
                if (!lineTxtStock.contains("HEADER") && (lineTxtStock.contains("MD001") || lineTxtStock.contains("MD002"))) {
                    Map<String, Long> volAmtMap = new HashMap<>();
                    String code = arr[1].trim();
                    long tradeVol = new Long(arr[3].trim()).longValue();
                    long tradeAmt = Double.valueOf(arr[4].trim()).longValue();
                    volAmtMap.put("tradeVol", tradeVol);
                    volAmtMap.put("tradeAmt", tradeAmt);
                    map.put(code, volAmtMap);
                }
            }
            brSzseStock.close();

        } catch (Exception e) {
            System.err.println("errors :" + e);
        }
        return map;
    }
}
