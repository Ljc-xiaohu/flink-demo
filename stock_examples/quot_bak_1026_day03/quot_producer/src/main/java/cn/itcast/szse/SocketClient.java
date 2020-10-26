package cn.itcast.szse;

import cn.itcast.avro.SzseAvro;
import cn.itcast.kafka.MyKafkaProducer;
import org.apache.commons.lang.SystemUtils;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Author itcast
 * Desc
 */
public class SocketClient {
    //0.定义一些常量
    private static String stockPath = "/export/servers/tmp//socket/szse-stock.txt";
    private static String indexPath = "/export/servers/tmp/socket/szse-index.txt";
    //随机浮动成交价格系数
    private static Double[] price = new Double[]{0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, -0.1, -0.11, -0.12, -0.13, -0.14, -0.15, -0.16, -0.17, -0.18, -0.19, -0.2};
    //随机浮动成交量
    private static int[] volumn = new int[]{50, 80, 110, 140, 170, 200, 230, 260, 290, 320, 350, 380, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300};
    //缓存产品代码、成交量和成交金额
    // code:"tradeVol":tradeVol
    // code:"tradeAmt":tradeAmt
    private static Map<String, Map<String, Long>> cacheMap = null;

    public static void main(String[] args) throws IOException {
        if(SystemUtils.IS_OS_WINDOWS){
            stockPath = "D:\\export\\servers\\tmp\\socket\\szse-stock.txt";
            indexPath = "D:\\export\\servers\\tmp\\socket\\szse-index.txt";
        }
        //1.建立socke连接获取流数据
        Socket socket = new Socket("localhost", 4444);
        InputStream inputStream = socket.getInputStream();
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        //2.读取文件并缓存产品代码、成交量和成交金额
        cacheMap = getVolAmtCacheMap();

        MyKafkaProducer myKafkaProducer = new MyKafkaProducer<>();

        while (true) {
            //3.解析数据并封装为bean对象
            String str = dataInputStream.readUTF();
            System.out.println(str);
            SzseAvro szseAvro = transform2Avro(str);
            System.out.println(szseAvro);
            //4.数据发送到Kafka
            myKafkaProducer.sendData("szse",szseAvro);
            System.out.println("数据已发送到Kafka");
        }
    }

    /**
     * 解析数据并封装为SzseAvro-bean对象
     */
    private static SzseAvro transform2Avro(String str) {
        //1.获取随机浮动成交量randomVol和随机浮动价格系数rondomPriceRate
        Random random = new Random();
        int randomVol = volumn[random.nextInt(volumn.length)];//随机浮动成交量
        Double rondomPriceRate = price[random.nextInt(price.length)];//随机浮动价格系数

        //2.字符串切割,获取1/9/7/8,即产品代码、上一次最新(成交)价、最高价、最低价
        String[] arr = str.split("\\|");
        String secCode = arr[1].trim();//产品代码
        BigDecimal tradePrice = new BigDecimal(arr[9].trim());//上一次成交价
        BigDecimal highPrice = new BigDecimal(arr[7].trim());//最高价
        BigDecimal lowPrice = new BigDecimal(arr[8].trim());//最低价

        //3.根据secCode从cacheMap中获取volAmtMap并获取成交量tradeVol和成交金额tradeAmt
        Map<String, Long> volAmtMap = cacheMap.get(secCode);
        Long tradeVol = volAmtMap.get("tradeVol");
        Long tradeAmt = volAmtMap.get("tradeAmt");

        //4.计算每秒成交金额
        //每秒成交金额tradeAmtSec = 本次最新价 * 本次随机成交量
        //每秒成交金额tradeAmtSec = (上一次成交价*(1+浮动系数rondomPriceRate)) * 本次随机成交量randomVol
        tradePrice = tradePrice.multiply(new BigDecimal(1 + rondomPriceRate)).setScale(2, RoundingMode.HALF_UP);//本次最新价
        BigDecimal tradeAmtSec = tradePrice.multiply(new BigDecimal(randomVol)).setScale(0, RoundingMode.HALF_UP);//每秒成交金额

        //5.计算最新的总成交量和总成交金额
        //总成交量tradeVolNew=tradeVol + randomVol
        //总成交金额tradeAmtNew=tradeAmt + 每秒成交金额tradeAmtSec
        Long tradeVolNew = tradeVol + randomVol;
        Long tradeAmtNew = tradeAmt + tradeAmtSec.longValue();

        //6.缓存最新的总成交量tradeVolNew和总成交金额tradeAmtNew到volAmtMap中并将secCode和volAmtMap更新到cacheMap
        volAmtMap.put("tradeVol", tradeVolNew);
        volAmtMap.put("tradeAmt", tradeAmtNew);
        cacheMap.put(secCode, volAmtMap);

        //7.获取最高价和最低价(和最新价比较)
        if (tradePrice.compareTo(highPrice) == 1) {
            highPrice = tradePrice;
        }
        if (tradePrice.compareTo(lowPrice) == -1) {
            lowPrice = tradePrice;
        }

        //8.封装结果数据到SzseAvro中
        SzseAvro szseAvro = new SzseAvro();
        //szseAvro.setMarket("szse");
        szseAvro.setMdStreamID(arr[0].trim());
        szseAvro.setSecurityID(arr[1].trim());
        szseAvro.setSymbol(arr[2].trim());
        szseAvro.setTradeVolume(tradeVolNew);
        szseAvro.setTotalValueTraded(tradeAmtNew);
        szseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));
        szseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));
        szseAvro.setHighPrice(highPrice.doubleValue());
        szseAvro.setLowPrice(lowPrice.doubleValue());
        szseAvro.setTradePrice(tradePrice.doubleValue());
        szseAvro.setClosePx(tradePrice.doubleValue());
        szseAvro.setTradingPhaseCode("T01");
        szseAvro.setTimestamp(new Date().getTime());

        //9.返回结果szseAvro
        return szseAvro;
    }

    /**
     * 缓存产品代码、成交量和成交金额
     */
    private static Map<String, Map<String, Long>> getVolAmtCacheMap() throws IOException {
        //1.创建一个map用来缓存结果
        // 缓存产品代码、成交量和成交金额
        // code:"tradeVol":tradeVol
        // code:"tradeAmt":tradeAmt
        Map<String, Map<String, Long>> map = new HashMap<>();
        //2.读取并缓存深市指数数据,注意:按|切割后取索引为1/3/4的数据,即产品代码、成交量和成交金额
        BufferedReader bosIndex = new BufferedReader(new InputStreamReader(new FileInputStream(new File(indexPath)), "UTF-8"));
        String indexStr;
        while ((indexStr = bosIndex.readLine()) != null) {
            String[] arr = indexStr.split("\\|");
            String code = arr[1].trim();
            Long tradeVol = Long.valueOf(arr[3].trim());
            long tradeAmt = Double.valueOf(arr[4].trim()).longValue();
            HashMap<String, Long> volAmtMap = new HashMap<>();
            volAmtMap.put("tradeVol", tradeVol);
            volAmtMap.put("tradeAmt", tradeAmt);
            map.put(code, volAmtMap);
        }
        bosIndex.close();

        //3.读取并缓存深市个股数据,注意:按|切割后取索引为1/3/4的数据,即产品代码、成交量和成交金额
        BufferedReader bosStock = new BufferedReader(new InputStreamReader(new FileInputStream(new File(stockPath)), "UTF-8"));
        String stockStr;
        while ((stockStr = bosStock.readLine()) != null) {
            String[] arr = stockStr.split("\\|");
            String code = arr[1].trim();
            Long tradeVol = Long.valueOf(arr[3].trim());
            long tradeAmt = Double.valueOf(arr[4].trim()).longValue();
            HashMap<String, Long> volAmtMap = new HashMap<>();
            volAmtMap.put("tradeVol", tradeVol);
            volAmtMap.put("tradeAmt", tradeAmt);
            map.put(code, volAmtMap);
        }
        bosStock.close();

        //4.返回map
        return map;
    }
}
