package cn.gp1996.gmall.flink.utils;

import cn.gp1996.gmall.flink.constants.PhoenixConfig;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * @author  gp1996
 * @date    2021-06-30
 * @desc    维度表工具类
 */
public class DimUtil {

    /**
     * 查询维度表的数据 V01,不考虑缓存
     * 维度表的主键一般为id
     * @param conn
     * @param table
     * @param pkValue  默认匹配id字段
     * @return
     */
    public static JSONObject query(Connection conn, String schema, String table, String pkValue) {

        JSONObject resDimInfo = null;
        Jedis jedis = null;

        try {
            // TODO 1.拼接sql | redisKey
            final StringBuilder sqlBuilder = new StringBuilder("select * from " + schema.toUpperCase() + ".");
            sqlBuilder.append(table.toUpperCase()).append(" where ").append("id = '").append(pkValue).append("'");
            final String sql = sqlBuilder.toString();
            final String key = table + ":" + pkValue;

            // TODO 2.在redis中先进行查询
            jedis = RedisUtil.getJedis();
            final String dimInfo = jedis.get(key);
            if (dimInfo == null) {
                // TODO 3.使用JDBCUtil进行查询Phoenix(预编译的sql+参数)
                final List<JSONObject> dimInfoList =
                        JDBCUtil.query(conn, sql, JSONObject.class, false);
                resDimInfo = dimInfoList.get(0);
                // TODO 4.往redis中写入当前读取的数据
                // NX 如果key不存在,进行设置, EX 秒,设置过期时间1s
                jedis.set(key, resDimInfo.toJSONString(), "NX", "EX", 60 * 60 * 24);
            } else {
                resDimInfo = JSONObject.parseObject(dimInfo);
            }
            System.out.println("resDimInfo: " + resDimInfo);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();// 归还连接
            }
        }

        // TODO 5.返回结果
        return resDimInfo;
    }

    /**
     * 删除Redis的数据(维度表的更新操作比较少，不需要在外面开连接了把？)
     * @param table
     * @param pkValue
     */
    public static void deleteOld(String table, String pkValue) {
        // Jedis jedis = null;
        // 获取jedis的连接
        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            // 从redis缓存中删除被更新的数据
            final String redisKey = table + ":" + pkValue;
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // 释放redis连接
            if (jedis != null) {
                jedis.close();
            }
        }

    }


    public static void main(String[] args) throws SQLException {
//        Connection conn = null;
//        final String tableName = "DIM_BASE_TRADEMARK";
//        final Properties phoenixConf = new Properties();
//        phoenixConf.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");
//        try {
//            // 获取Phoenix连接
//            Class.forName(PhoenixConfig.PHOENIX_DRIVER);
//            conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER, phoenixConf);
//            conn.setSchema(PhoenixConfig.HBASE_SCHEMA);
//            final long t1 = System.currentTimeMillis();
//            final JSONObject dimInfo1 = query(conn, tableName, "1");
//            final long t2 = System.currentTimeMillis();
//            System.out.println(dimInfo1.toString() + " " + (t2 - t1));
//            final long t3 = System.currentTimeMillis();
//            final JSONObject dimInfo2 = query(conn, tableName, "1");
//            final long t4 = System.currentTimeMillis();
//            System.out.println(dimInfo2 + " " + (t4 - t3));
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        } finally {
//            if (conn != null) {
//                conn.close();
//            }
//        }
    }
}
