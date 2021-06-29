package cn.gp1996.gmall.flink.app.ods;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.CDC_DB_DeserializationSchema;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author  gp1996
 * @date    2021-06-25
 * @desc    使用Flink-CDC同步gmall业务数据到kafka
 *          topic：ods_base_db
 */
public class ODS_DB_CDC {
    public static void main(String[] args) {

        // 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.设置ck
//        env.enableCheckpointing(GmallConstants.DEFAULT_CK_INTERNAL, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend(
//                GmallConstants.FLINK_APP_CK_PATH + "/" + ODS_DB_CDC.class.getSimpleName()));
        // 任务取消时保留checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置job重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        // 3.获取cdc数据源
        final DebeziumSourceFunction<String> dbSourceFunction = MySQLSource.
                <String>builder()
                .hostname("hadoop111")
                .port(3306)
                .username("root")
                .password("qqfdjkd12")
                .databaseList("gmall_flink")
                //.tableList("gmall_flink.base_trademark") // -> 相当于监控库下所有表
                .tableList("gmall_flink.order_info,gmall_flink.order_detail")
                .deserializer(new CDC_DB_DeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        final DataStreamSource<String> dbChangeStream = env.addSource(dbSourceFunction);

        // 4.输出数据到kafka中
        dbChangeStream.addSink(KafkaUtil.getProducer(GmallConstants.ODS_BASE_DB));
        dbChangeStream.print("change-log");

        // 5.执行job
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
