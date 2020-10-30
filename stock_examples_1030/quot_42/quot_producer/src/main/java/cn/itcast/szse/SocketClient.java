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
 * Date 2020/10/23 15:04
 * Desc
 * 接收localhost:4444对外广播的深市个股和指数行情数据
 */
public class SocketClient {
    //0.定义一些常量后续需要使用
    private static String stockPath = "/export/servers/tmp//socket/szse-stock.txt";//深市个股数据路径
    private static String indexPath = "/export/servers/tmp/socket/szse-index.txt";//深市指数数据路径
    //定义一些随机浮动成交价格系数
    private static Double[] prices = new Double[]{0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, -0.1, -0.11, -0.12, -0.13, -0.14, -0.15, -0.16, -0.17, -0.18, -0.19, -0.2};
    //定义一些随机浮动成交量
    private static int[] volumns = new int[]{50, 80, 110, 140, 170, 200, 230, 260, 290, 320, 350, 380, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300};
    //定义一个Map用来缓存产品代码、成交量和成交金额
    // code:"tradeVol":tradeVol
    // code:"tradeAmt":tradeAmt
    //如:
    //Map<600015, Map<tradeVol, 1000>>
    //Map<600015, Map<tradeAmt, 9999.99>>
    private static Map<String, Map<String, Long>> cacheMap = null;

    public static void main(String[] args) throws Exception {
        //开发步骤：
        //0.判断是Windows还是Liunx
        if(SystemUtils.IS_OS_WINDOWS){
            stockPath = "D:\\export\\servers\\tmp\\socket\\szse-stock.txt";
            indexPath = "D:\\export\\servers\\tmp\\socket\\szse-index.txt";
        }
        //1.建立socket连接localhost:4444获取流数据
        Socket socket = new Socket("localhost", 4444);
        InputStream inputStream = socket.getInputStream();
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        //加载本地文件数据并缓存产品代码、成交量和成交金额
        //code:"tradeVol":tradeVol
        //code:"tradeAmt":tradeAmt
        //如:
        //Map<600015, Map<tradeVol, 1000>>
        //Map<600015, Map<tradeAmt, 9999.99>>
        //2.读取文件并缓存产品代码、成交量和成交金额
        //并参与后面的随机数据生成,比记录当前产品代码最新的成交量和成交金额供下一次使用!
        cacheMap = getVolAmtCacheMap();

        //准备Kafka生产者对象
        MyKafkaProducer myKafkaProducer = new MyKafkaProducer();

        //System.out.println("获取到的cacheMap为:"+cacheMap);
        while (true){
            String str = dataInputStream.readUTF();
            //System.out.println("从深市socket服务端接收到的数据为:"+str);
            //3.循环接收并解析数据并封装为bean对象,也就是String-->SzseAvro
            SzseAvro szseAvro = transform2Avro(str);
            //System.out.println("将String转为SzseAvro的结果为:" + szseAvro);
            //4.数据发送到Kafka
            myKafkaProducer.sendData("szse",szseAvro);
            System.out.println("数据已序列化并发送到Kafka");
    }
    }

    /**
     * 用来将String-->SzseAvro
     *  注意:BigDecimal类表示精确的double,使用方法是完全的面向对象
     *  +:add
     *  -:subtract
     *  +:multiply
     *  /:divide
     */
    private static SzseAvro transform2Avro(String str) {
        //注意:每秒从SocketServer接收到的数据都一样,但是我们在这里要把数据根据随机的浮动成交量和成交金额把数据模拟成变化的
        //开发步骤：
        //1.获取随机浮动成交量randomVol和随机浮动价格系数rondomPriceRate
        Random random = new Random();
        int randVolumn = volumns[random.nextInt(volumns.length)];//随机浮动成交量
        double randpPiceRate = prices[random.nextInt(prices.length)];//随机浮动价格系数

        //2.字符串切割,获取1/9/7/8,即产品代码、上一次最新(成交)价、最高价、最低价
        String[] arr = str.split("\\|");
        String code = arr[1].trim();//产品代码
        BigDecimal lastPrice = new BigDecimal(arr[9].trim());//上一次最新(成交)价
        BigDecimal highPrice = new BigDecimal(arr[7].trim());
        BigDecimal lowPrice = new BigDecimal(arr[8].trim());

        //3.根据secCode从cacheMap中获取volAmtMap并获取成交量tradeVol和成交金额tradeAmt
        //cacheMap
        // code:"tradeVol":tradeVol
        // code:"tradeAmt":tradeAmt
        //如:
        //Map<600015, Map<tradeVol, 1000>>
        //Map<600015, Map<tradeAmt, 9999.99>>
        Map<String, Long> volAmtMap = cacheMap.get(code);
        Long tradeVol = volAmtMap.get("tradeVol");//缓存中的成交量
        Long tradeAmt = volAmtMap.get("tradeAmt");//缓存中的成交金额

        //4.计算每秒成交金额
        // 每秒成交金额tradeAmtSec = 本次最新价 * 本次随机成交量
        // 每秒成交金额tradeAmtSec = (上一次成交价*(1+浮动系数rondomPriceRate)) * 本次随机成交量randomVol
        BigDecimal currentPrice = lastPrice.multiply(new BigDecimal(1 + randpPiceRate));//本次最新价
        BigDecimal tradeAmtSec = currentPrice
                .multiply(new BigDecimal(randVolumn))
                .setScale(2, RoundingMode.HALF_UP);//四舍五入

        //5.计算最新的总成交量和总成交金额
        // 总成交量 = 上一次的成交量 + 这一次的成交量
        // 总成交量 = 缓存中的成交量 + 这一次随机的成交量
        // tradeVolNew=tradeVol + randomVol
        Long tradeVolNew = tradeVol + randVolumn;
        // 总成交金额 = 上一次的成交金额 + 这一次的成交金额
        // 总成交金额 = 缓存中的成交金额 + 这一次随机浮动的成交金额
        //  tradeAmtNew=tradeAmt + 每秒成交金额tradeAmtSec
        BigDecimal tradeAmtNew = tradeAmtSec.add(new BigDecimal(tradeAmt));

        //6.缓存最新的总成交量tradeVolNew和总成交金额tradeAmtNew到volAmtMap中并将secCode和volAmtMap更新到cacheMap
        volAmtMap.put("tradeVol",tradeVolNew);
        volAmtMap.put("tradeAmt",tradeAmtNew.longValue());
        cacheMap.put(code,volAmtMap);

        //7.获取当前最高价和最低价(和最新价比较)
        //注意:
        //A.compareTo(B)表示A和B比较
        //A.compareTo(B) == 1 表示A>B
        //A.compareTo(B) == -1 表示A<B
        if(currentPrice.compareTo(highPrice) == 1){//currentPrice > highPrice
            highPrice = currentPrice;
        }
        if(currentPrice.compareTo(lowPrice) == -1){//currentPrice < lowPrice
            lowPrice = currentPrice;
        }

        //8.封装结果数据到SzseAvro中
        SzseAvro szseAvro = new SzseAvro();
        szseAvro.setMdStreamID(arr[0].trim());//行情数据类型
        szseAvro.setSecurityID(code);//产品代码
        szseAvro.setSymbol(arr[2].trim());//产品名称
        szseAvro.setTradeVolume(tradeVolNew);//目前总成交量
        szseAvro.setTotalValueTraded(tradeAmtNew.longValue());//目前总成金额
        szseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));//前收盘价
        szseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));//开盘价
        szseAvro.setHighPrice(highPrice.doubleValue());//最高价
        szseAvro.setLowPrice(lowPrice.doubleValue());//最低价
        szseAvro.setTradePrice(currentPrice.doubleValue());//最新价
        szseAvro.setClosePx(currentPrice.doubleValue());//收盘价,如果还没有闭市,当前最新价就是收盘价
        szseAvro.setTradingPhaseCode("T01");//实时阶段及标志
        szseAvro.setTimestamp(new Date().getTime());//时间戳

        //9.返回结果szseAvro
        return szseAvro;
    }

    /**
     * 解析文件获取成交量和成交金额数据，并封装到Map<String, Map<String, Long>> cacheMap中。
     */
    private static Map<String, Map<String, Long>> getVolAmtCacheMap() throws Exception {
        //开发步骤：
        //1.创建一个map用来缓存结果
        // 缓存产品代码、成交量和成交金额
        // code:"tradeVol":tradeVol
        // code:"tradeAmt":tradeAmt
        //如:
        //Map<600015, Map<tradeVol, 1000>>
        //Map<600015, Map<tradeAmt, 9999.99>>
        Map<String, Map<String, Long>> cacheMap = new HashMap<>();
        //2.读取并缓存深市指数数据,注意:按\|切割后取索引为1/3/4的数据,即产品代码、成交量和成交金额
        BufferedReader indexReader = new BufferedReader(new InputStreamReader(new FileInputStream(indexPath)));
        String indexStr = null;
        while ((indexStr = indexReader.readLine()) != null){
            String[] arr = indexStr.split("\\|");
            String code = arr[1].trim();
            long tradeVol = Long.parseLong(arr[3].trim());
            long tradeAmt = new BigDecimal(arr[4].trim()).longValue();
            HashMap<String, Long> map = new HashMap<>();
            map.put("tradeVol",tradeVol);
            map.put("tradeAmt",tradeAmt);
            cacheMap.put(code,map);
        }
        //3.读取并缓存深市个股数据,注意:按\|切割后取索引为1/3/4的数据,即产品代码、成交量和成交金额
        BufferedReader stockReader = new BufferedReader(new InputStreamReader(new FileInputStream(stockPath)));
        String stockStr = null;
        while ((stockStr = stockReader.readLine()) != null){
            String[] arr = stockStr.split("\\|");
            String code = arr[1].trim();
            long tradeVol = Long.parseLong(arr[3].trim());
            long tradeAmt = new BigDecimal(arr[4].trim()).longValue();
            HashMap<String, Long> map = new HashMap<>();
            map.put("tradeVol",tradeVol);
            map.put("tradeAmt",tradeAmt);
            cacheMap.put(code,map);
        }

        //4.返回map
        return cacheMap;
    }
}
