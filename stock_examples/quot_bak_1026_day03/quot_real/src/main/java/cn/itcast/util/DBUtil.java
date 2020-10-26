package cn.itcast.util;

import cn.itcast.config.QuotConfig;

import java.sql.*;
import java.util.*;


/**
 * MySQL工具类
 */
public class DBUtil {
    /**
     * 根据sql更新表数据
     */
    public static void update(String sql) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DBUtil.getConnByJdbc();
            statement = connection.createStatement();
            statement.executeUpdate(sql);

        } catch (Exception e) {
            System.out.println("Query Database error!\n" + e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 根据key和sql查询单行行Map<key,row>
     * 可以复合key
     */
    public static Map<String, Map<String, Object>> query(String key, String sql) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        Map<String, Map<String, Object>> table = new HashMap<>();
        String[] keys = key.split(",");
        try {
            connection = DBUtil.getConnByJdbc();
            preparedStatement = connection.prepareStatement(sql);
            rs = preparedStatement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            int colNum = rsm.getColumnCount();
            while (rs.next()) {

                Map<String, Object> row = new HashMap<>();

                for (int i = 1; i <= colNum; i++) {
                    row.put(rsm.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                String keyVal = "";
                for (String val : keys) {
                    keyVal += String.valueOf(row.get(val.toLowerCase())).trim();
                }

                if (keyVal.equalsIgnoreCase("")) {
                    System.out.println("keyval is empty");
                    continue;
                }

                table.put(keyVal, row);
            }
        } catch (Exception e) {
            System.out.println("Query Database error!\n" + e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return table;
    }

    /**
     * 根据key和sql查询多行Map<key, List<row>>
     */
    public static Map<String, List<Map<String, Object>>> queryForGroup(String key, String sql) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        // 输出结果集
        Map<String, List<Map<String, Object>>> res = new HashMap<>();
        try {
            connection = DBUtil.getConnByJdbc();
            preparedStatement = connection.prepareStatement(sql);
            rs = preparedStatement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            // 获取列数
            int colNum = rsm.getColumnCount();

            while (rs.next()) {
                // 获取keyVal
                String keyVal = rs.getString(key);
                if (res.containsKey(keyVal)) {
                    List<Map<String, Object>> oldRows = res.get(keyVal);
                    Map<String, Object> cols = new HashMap<>();
                    for (int i = 1; i <= colNum; i++) {
                        cols.put(rsm.getColumnName(i).toLowerCase(), rs.getObject(i));
                    }
                    oldRows.add(cols);
                } else {
                    List<Map<String, Object>> newRows = new ArrayList<>();
                    Map<String, Object> cols = new HashMap<>();
                    for (int i = 1; i <= colNum; i++) {
                        cols.put(rsm.getColumnName(i).toLowerCase(), rs.getObject(i));
                    }
                    newRows.add(cols);

                    if (keyVal.equalsIgnoreCase("")) {
                        System.out.println("keyval is empty");
                        continue;
                    }
                    res.put(keyVal, newRows);
                }
            }
        } catch (Exception e) {
            System.out.println("Query Database error!");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return res;
    }

    /**
     * 根据sql查询单行Map<String, String>
     */
    public static Map<String, String> queryKv(String sql) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;

        // 输出结果集
        Map<String, String> map = new HashMap<>();

        try {
            connection = getConnByJdbc();
            preparedStatement = connection.prepareStatement(sql);
            rs = preparedStatement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            int colNum = rsm.getColumnCount();
            while (rs.next()) {
                int i = 1;
                while (i <= colNum) {
                    map.put(rsm.getColumnName(i).toLowerCase(), rs.getString(i));
                    i += 1;
                }
            }
        } catch (Exception e) {
            System.out.println("Query Database error!");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return map;
    }

    /**
     * 根据sql查询多行List<Map<String, String>>
     */
    public static List<Map<String, String>> queryKvList(String sql) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        // 输出结果集
        List<Map<String, String>> list = new ArrayList<>();
        try {
            connection = getConnByJdbc();
            preparedStatement = connection.prepareStatement(sql);
            rs = preparedStatement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            int colNum = rsm.getColumnCount();
            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                int i = 1;
                while (i <= colNum) {
                    map.put(rsm.getColumnName(i).toLowerCase(), rs.getString(i));
                    list.add(map);
                    i += 1;
                }
            }
        } catch (Exception e) {
            System.out.println("Query Database error!");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

    /**
     * 获取连接
     */
    public static Connection getConnByJdbc() {
        Connection conn = null;
        try {
            //Class.forName(QuotConfig.config.getProperty("mysql.driver"));
            conn = DriverManager.getConnection(
                    QuotConfig.config.getProperty("mysql.url"),
                    QuotConfig.config.getProperty("mysql.username"),
                    QuotConfig.config.getProperty("mysql.password"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
