package cn.gp1996.gmall.flink.app.dwd;

import cn.gp1996.gmall.flink.app.func.PeriodicPullTableProcessFunction;
import cn.gp1996.gmall.flink.app.func.ValidJsonAndConvertProcessFunction;
import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试使用周期性拉取
 */
public class BaseDBAppV2 {

    public static void main(String[] args) {
        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置ck
//         env.enableCheckpointing(GmallConstants.DEFAULT_CK_INTERNAL, CheckpointingMode.EXACTLY_ONCE);
//         env.setStateBackend(new FsStateBackend(GmallConstants.FLINK_APP_CK_PATH + "/" + BaseDBApp.class.getSimpleName()));
        // 设置外化ck的策略
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

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

        // TODO 测试周期性获取 Timer ? 定时调度线程池 ?
        final SingleOutputStreamOperator<String> resStream =
                validBaseDBStream
                        .keyBy(new KeySelector<JSONObject, String>() {
                            @Override
                            public String getKey(JSONObject value) throws Exception {
                                return value.getString("sourceTable") + "_" + value.getString("type");
                            }
                        })
                        .process(new PeriodicPullTableProcessFunction());

        resStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
