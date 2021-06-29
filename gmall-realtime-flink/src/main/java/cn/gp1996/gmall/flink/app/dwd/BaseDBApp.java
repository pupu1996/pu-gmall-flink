package cn.gp1996.gmall.flink.app.dwd;

import cn.gp1996.gmall.flink.app.func.*;
import cn.gp1996.gmall.flink.bean.TableProcess;
import cn.gp1996.gmall.flink.constants.FlinkCDCConstants;
import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.CDC_DB_DeserializationSchema;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Json;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc    数据源： ods_base_db
 *          需求1：处理Flink-CDC接收的数据，对data值为空的数据进行过滤(etl)
 */
public class BaseDBApp {

    // 定义配置表状态描述符
    private static final MapStateDescriptor<String, TableProcess>
                        tableProcessStateDesc = new MapStateDescriptor<String, TableProcess>(
                            "tableProcessState",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            TypeInformation.of(TableProcess.class)
            );

    // 定义维度表侧输出流outTag
    private static final OutputTag<JSONObject> dimOT =
            new OutputTag<>(
                    GmallConstants.DIM_OUTPUT,
                    TypeInformation.of(JSONObject.class));

    public static void main(String[] args) throws Exception {

        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置ck
//        env.enableCheckpointing(GmallConstants.DEFAULT_CK_INTERNAL, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend(
//                 GmallConstants.FLINK_APP_CK_PATH + "/" + BaseDBApp.class.getSimpleName()));
//        // 设置外化ck的策略
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // TODO 3.其他配置
        // 设置访问hdfs的用户
        System.setProperty("HADOOP_USER_NAME", "gp1996");

        // TODO 4.从kafka获取ods_base_db的数据
        final DataStreamSource<String> baseDBStream =
                env.addSource(KafkaUtil.getConsumer(
                        GmallConstants.ODS_BASE_DB,
                        GmallConstants.GROUP_BASE_DB_APP
            ));

        // TODO 5.过滤错误的json数据，并转换
        final SingleOutputStreamOperator<JSONObject> baseDBObjStream = baseDBStream
                .process(new ValidJsonAndConvertProcessFunction(true));

        // TODO 6.过滤掉data非法的数据，并转换
        final SingleOutputStreamOperator<JSONObject> validBaseDBStream = baseDBObjStream
                .filter(new FilterNullDataFunction());

        // 测试输出
        // validBaseDBStream.print("valid");
        // validBaseDBStream.getSideOutput(
        //        ValidJsonAndConvertProcessFunction.DEFAULT_PARSE_ERR_OT).print();

        // TODO 7.读取配置表数据,将其转换为广播流
        final DebeziumSourceFunction<String> tableProcessSource = MySQLSource
                .<String>builder()
                .hostname(FlinkCDCConstants.CDC_CONN_HOST_NAME)
                .port(FlinkCDCConstants.CDC_CONN_PORT)
                .username(FlinkCDCConstants.CDC_CONN_USER_NAME)
                .password(FlinkCDCConstants.CDC_CONN_PWD)
                .databaseList(FlinkCDCConstants.WATCHED_TABLE_PROCESS_DB)
                .tableList(FlinkCDCConstants.WATCHED_TABLE_PROCESS)
                .startupOptions(StartupOptions.initial())
                .deserializer(new CDC_DB_DeserializationSchema())
                .build();
        final DataStreamSource<String> tableProcessStream =
                env.addSource(tableProcessSource);
        final BroadcastStream<String> bcTableProcessStream =
                tableProcessStream.broadcast(tableProcessStateDesc);

        // TODO 8.与ods_base_db业务数据流进行connect
        // 根据主键join配置表规则，以配置信息进行建表(phoenix需要提前建表，kafka自动创建)
        // 将数据按kafka事实表和hbase维度表进行分流(实时表主流，维度表侧输出流)
        final SingleOutputStreamOperator<JSONObject> dwdSplitStream = validBaseDBStream.connect(bcTableProcessStream)
                .process(
                        new SplitFactAndDimBroadcastProcessFunction(
                                tableProcessStateDesc,
                                dimOT
                ));

        // 事实|维度分流打印数据
        dwdSplitStream.print("kafka");
        final DataStream<JSONObject> dimStream = dwdSplitStream.getSideOutput(dimOT);
        dimStream.print("hbase");

        // TODO 9.将kafka事实表数据写入对应主题
        dwdSplitStream.addSink(KafkaUtil.getProducerWithCustomSchema(new DwdFactSerializationSchema()));

        // TODO 10.将hbase维度表数据写入对应主题
        dimStream.addSink(new PhoenixSinkFunction());


        // TODO final.执行
        env.execute();
    }

    /**
     * 过滤掉data为null的数据
     */
    private static class FilterNullDataFunction
            implements FilterFunction<JSONObject> {
        @Override
        public boolean filter(JSONObject value) throws Exception {

            final String data = value.getString("data");
            return data != null && data.length() > 2;
        }
    }

}
