package cn.itcast.szse;


import org.apache.commons.lang.SystemUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * 深市行情实时行情广播数据
 */
public class SocketServer {
    private static String stockPath = "/export/servers/tmp//socket/szse-stock.txt";
    private static String indexPath = "/export/servers/tmp/socket/szse-index.txt";
    private static final int PORT = 4444;
    public static void main(String[] args) {
        if(SystemUtils.IS_OS_WINDOWS){
            stockPath = "D:\\export\\servers\\tmp\\socket\\szse-stock.txt";
            indexPath = "D:\\export\\servers\\tmp\\socket\\szse-index.txt";
        }

        ServerSocket server ;
        Socket socket ;
        DataOutputStream out ;
        try {
            server = new ServerSocket(PORT, 1000, InetAddress.getByName("localhost"));
            socket = server.accept();
            out = new DataOutputStream(socket.getOutputStream());
            while (true) {
                System.out.println("服务端每隔1s广播一次行情数据");
                Thread.sleep(1000);//服务端1秒钟，广播一次行情数据
                List<String> list = parseIndexFile();
                for (Object ling : list) {
                    out.writeUTF(ling.toString());
                }
                List<String> list1 = parseStockFile();
                for (Object line : list1) {
                    out.writeUTF(line.toString());
                }
                out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 个股行情
     */
    private static List<String> parseStockFile() {
        ArrayList<String> list = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(stockPath)),"UTF-8"));
            String lineTxt ;
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
     * 指数行情
     */
    public static List<String> parseIndexFile() {
        ArrayList<String> list = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(indexPath)),"UTF-8"));
            String lineTxt ;
            while ((lineTxt = br.readLine()) != null) {
                list.add(lineTxt);
            }
            br.close();
        } catch (Exception e) {
            System.err.println("errors :" + e);
        }
        return list;
    }
}