package cn.gp1996.gmall.flink.utils;

import cn.gp1996.gmall.flink.constants.PhoenixConfig;
import com.alibaba.fastjson.JSONObject;
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

    // 通用的查询
    public static <T> List<T> query(
            Connection conn, String sql,Class<T> clz, boolean needConvert) {

        // TODO 1.创建返回对象
        final LinkedList<T> queryResList = new LinkedList<>();

        PreparedStatement pst = null;

        try {
            // TODO 2.预编译sql
            pst = conn.prepareStatement(sql);

            // TODO 3.执行查询
            final ResultSet resultSet = pst.executeQuery();

            // TODO 4.解析结果
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                T row = parseColObj(resultSet, metaData, clz, needConvert);
                queryResList.add(row);
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
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

    public static void main(String[] args) throws ClassNotFoundException {
        // 测试
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        Connection conn = null;
        try {
            final Properties phoenixConf = new Properties();
            phoenixConf.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");
            conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER, phoenixConf);
            final String sql = "select * from gmall_flink_dim.dim_base_trademark";
            final List<JSONObject> queryRes = JDBCUtil.query(conn, sql, JSONObject.class, true);
            System.out.println(queryRes.toString());

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
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
