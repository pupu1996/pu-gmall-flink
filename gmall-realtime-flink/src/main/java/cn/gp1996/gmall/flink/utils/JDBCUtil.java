package cn.gp1996.gmall.flink.utils;

import cn.gp1996.gmall.flink.bean.OrderInfo;
import com.google.common.base.CaseFormat;
import com.google.gson.JsonObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.BeanUtilsBean;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * @author  gp1996
 * @date    2021-06-29
 * @desc    通用的jdbc描述器
 */
public class JDBCUtil {

    /**
     * 通用的jdbc查询
     * @param conn  jdbc连接
     * @param sql   查询sql语句
     * @param clz   传入泛型的Class实例，通过他来创建对象
     * @param needConvert  查询得到的列名是否要进行下划线->驼峰的转换(需要保证表列名和javaBean属性名命名方式相同，才能使用反射
     *                     表列名通常为下划线，javaBean通常为驼峰)
     * @param <T>
     * @return
     */
    public static <T> List<T> query(
            Connection conn, String sql,Class<T> clz, boolean needConvert) {

        // TODO 1.创建返回对象
        final LinkedList<T> queryResList = new LinkedList<>();

        PreparedStatement pst = null;
        ResultSet resultSet = null;

        try {
            // TODO 2.预编译sql
            pst = conn.prepareStatement(sql);

            // TODO 3.执行查询
            resultSet = pst.executeQuery();

            // TODO 4.解析结果
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                T row = parseColObj(resultSet, metaData, clz, needConvert);
                queryResList.add(row);
            }

            // System.out.println(queryResList.toString());

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        // TODO final 输出返回结果
        return queryResList;
    }


    /***
     * 解析列对象
     * @param resultSet
     * @param <T>
     * @return
     */
    private static <T> T parseColObj(
            ResultSet resultSet, ResultSetMetaData metaData, Class<T> clz, boolean needConvert)
    {
        T t = null;
        try {
            // 创建返回对象
            t = clz.newInstance();
            // 获取列数
            final int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                // 获取列名
                String columnName = metaData.getColumnName(i);
                if (needConvert) {
                    columnName =
                            CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 获取列值
                final String colValue = resultSet.getString(i);
                BeanUtils.setProperty(t, columnName, colValue);
            }

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return t;

    }

//    public static void main(String[] args) throws ClassNotFoundException {
//        // 测试查询phoenix
//        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
//        Connection conn = null;
//        try {
//            final Properties phoenixConf = new Properties();
//            phoenixConf.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");
//            conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER, phoenixConf);
//            final String sql = "select * from gmall_flink_dim.dim_base_trademark";
//            final List<JSONObject> queryRes = JDBCUtil.query(conn, sql, JSONObject.class, true);
//            System.out.println(queryRes.toString());
//
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
//                } catch (SQLException throwables) {
//                    throwables.printStackTrace();
//                }
//            }
//        }
//    }

    public static void main(String[] args) {
        // 1.测试查询mysql
        Connection conn = null;
        PreparedStatement pst = null;
        try {
            // 创建连接
            Class.forName("com.mysql.jdbc.Driver");// 通过类加载器加载jdbc驱动
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop111:3306/gmall_flink?useSSL=false",
                    "root",
                    "qqfdjkd12");
            final String sql = "select * from order_info";
            // 预编译sql
            final List<OrderInfo> res = JDBCUtil.query(conn, sql, OrderInfo.class, false);
            System.out.println(res.toString());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }
}
