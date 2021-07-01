package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.constants.PhoenixConfig;
import cn.gp1996.gmall.flink.utils.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author  gp1996
 * @date    2021-06-30
 * @desc    定义通用的异步维表joinFunction，每个实例负责一张表的join
 */
public abstract class GenericAsyncJoinFunction<T>
        extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T>{

    // 线程池引用
    protected ThreadPoolExecutor threadPool = null;

    //JDBC连接对象引用
//    protected Connection conn = null;

    // 保存需要sink的表名
    protected String dimTableName = null;

    public GenericAsyncJoinFunction(String tableName) {
        this.dimTableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO 1.初始化线程池
        threadPool = ThreadPoolUtil.getCachedInstance();
        // TODO 2.初始化jedis连接池
        RedisUtil.init();
        // TODO 3.创建JDBC连接
//        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
//        final Properties phoenixConf = new Properties();
//        phoenixConf.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");
//        conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER, phoenixConf);
//        // 使用schema
//        conn.setSchema(PhoenixConfig.HBASE_SCHEMA);
        // TODO 3.使用连接池?
         // 初始化连接池
         DruidUtil.init();
    }

    // 使用连接池的版本
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // T input 当前输入的数据
        final Runnable joinTask = new Runnable() {
            @Override
            public void run() {
                // TODO 0.从Druid连接池中获取连接
                System.out.println("---------" + Thread.currentThread() + " 准备发送请求---------");
                Connection conn = null;
                try {
                    conn = DruidUtil.getConnection();
                    // TODO 1.根据输入的数据获取id getKey
                    String value = getKeyValue(input);
                    // TODO 2.根据表名+id查询维度表信息
                    final JSONObject dimInfo = DimUtil.query(conn, PhoenixConfig.HBASE_SCHEMA, dimTableName, value);
                    // TODO 3.join
                    if (dimInfo != null && dimInfo.size() > 0) {
                        join(input, dimInfo);
                    }

                }catch (ParseException | SQLException throwables) {
                    throwables.printStackTrace();
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        } finally {
                            conn = null;
                        }
                    }
                }
//                System.out.println("------- join完成 -------");
                resultFuture.complete(Collections.singletonList(input));
            }
        };

        // 提交任务
        threadPool.submit(joinTask);
    }

    // 不使用连接池的版本
//    @Override
//    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
//        // T input 当前输入的数据
//        final Runnable joinTask = new Runnable() {
//            @Override
//            public void run() {
//                // TODO 0.从Druid连接池中获取连接
//                System.out.println("---------" + Thread.currentThread() + " 准备发送请求---------");
//                // Connection conn = null;
//                try {
//                    // conn.setSchema(PhoenixConfig.HBASE_SCHEMA);
//                    // TODO 1.根据输入的数据获取id getKey
//                    String value = getKeyValue(input);
//                    // TODO 2.根据表名+id查询维度表信息
//                    final JSONObject dimInfo = DimUtil.query(conn, PhoenixConfig.HBASE_SCHEMA, dimTableName, value);
//                    // TODO 3.join
//                    if (dimInfo != null && dimInfo.size() > 0) {
//                        join(input, dimInfo);
//                    }
//
//                }catch (ParseException throwables) {
//                    throwables.printStackTrace();
//                } finally {
////                    if (conn != null) {
////                        try {
////                            conn.close();
////                        } catch (SQLException throwables) {
////                            throwables.printStackTrace();
////                        }
////                    }
//                }
////                System.out.println("------- join完成 -------");
//                resultFuture.complete(Collections.singletonList(input));
//            }
//        };
//
//        // 提交任务
//        threadPool.submit(joinTask);
//    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println(input + ">>>>>>>>>>>>>>>>超时了！！！");
    }

    @Override
    public void close() throws Exception {
        // 关闭资源
//        if (conn != null) {
//            conn.close();
//        }

    }
}
