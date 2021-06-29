package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.bean.TableProcess;
import cn.gp1996.gmall.flink.utils.JDBCUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 拉取线程写，数据task线程读(并发操作)
 */
public class PeriodicPullTableProcessFunction extends KeyedProcessFunction<String, JSONObject,String> {

    // 创建定时调度线程池对象
    private ScheduledExecutorService tp;

    // 创建保存tableProcess数据hashmap
    private ConcurrentHashMap<String, TableProcess> tpMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化定时调度线程池
        tp = Executors.newScheduledThreadPool(1);
        // 初始化状态
        // tpState = getRuntimeContext().getMapState(tpStateDesc);
        // 定时调度周期性拉取任务
        tpMap = new ConcurrentHashMap<>(256);
        tp.scheduleWithFixedDelay(new PullTableProcessTask(tpMap), 0, 20, TimeUnit.SECONDS);
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
        // 获取源表 + 操作
        final String tableName = value.getString("tableName");
        final String type = value.getString("type");
        // 拼接
        final String key = tableName + "_" + type;
        // System.out.println(tpMap.toString());
        final TableProcess tableProcess = tpMap.get(key);
        // System.out.println("tableProcess: " + tableProcess);
        if (tableProcess != null) {
            final JSONObject data = value.getJSONObject("data");
            data.put("sinkTable", tableProcess.getSinkTable());
            out.collect(data.toJSONString());
        } else {
            System.out.println("不能匹配!");
        }

    }

    // 拉取
    private static class PullTableProcessTask implements Runnable {

        private final ConcurrentHashMap<String, TableProcess> tpMap;

        PullTableProcessTask(ConcurrentHashMap<String, TableProcess> tpMap) {
            this.tpMap = tpMap;
        }

        @Override
        public void run() {
            
            Connection conn = null;
            PreparedStatement pst = null;
            try {
                // 1.创建与数据库的连接
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://hadoop111:3306/gmall_flink_extend?useSSL=false",
                        "root",
                        "qqfdjkd12"
                );
                String dql = "select * from table_process";
                JDBCUtil.query(conn, dql, TableProcess.class, true);
            } catch (SQLException | ClassNotFoundException throwables) {
                throwables.printStackTrace();
            }

            // 2.预编译sql



        }

    }
}
