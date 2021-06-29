package cn.gp1996.gmall.flink.app.dwd;

import cn.gp1996.gmall.flink.app.func.ValidJsonProcessFunction;
import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author  gp1996
 * @date    2021-06-25
 * @desc    数据源：ods_base_log
 *          需求1：校验新老用户
 *          需求2：对不同类型的日志数据进行分流
 */
public class BaseLogApp {

    // 定义启动日志的ot tag
    private static final OutputTag<String> startLogOT = new OutputTag<>(
            GmallConstants.OT_START_LOG,
            BasicTypeInfo.STRING_TYPE_INFO
    );

    // 定义曝光日志的ot tag
    private static final OutputTag<String> displayLogOT = new OutputTag<>(
            GmallConstants.OT_DISPLAY_LOG,
            BasicTypeInfo.STRING_TYPE_INFO
    );

    // 定义脏数据的ot tag
    private static final OutputTag<String> dirtyLogOT = new OutputTag<>(
    GmallConstants.OT_ERROR_BASE_LOG_APP,
    BasicTypeInfo.STRING_TYPE_INFO
                );

    public static void main(String[] args) {

        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置ck
//        env.enableCheckpointing(GmallConstants.DEFAULT_CK_INTERNAL, CheckpointingMode.EXACTLY_ONCE);
        // 任务完全失败或cancel时可以保留下ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 设置状态后端
         env.setStateBackend(new FsStateBackend(
                GmallConstants.FLINK_APP_CK_PATH + "/" + BaseLogApp.class.getSimpleName()));

        // 设置操作hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "gp1996");

        // TODO 3.从kafka中获取数据源，主题ods_base_log
        final DataStreamSource<String> baseLogStream =
                env.addSource(KafkaUtil.getConsumer(
                GmallConstants.ODS_BASE_LOG,
                GmallConstants.GROUP_BASE_LOG_APP));

        // TODO 4.过滤脏数据，将脏数据输出到侧输出流中
        final SingleOutputStreamOperator<String> validBaseLogStream =
                baseLogStream
                .process(new ValidJsonProcessFunction(
                        true,
                        GmallConstants.OT_ERROR_BASE_LOG_APP
                ));

        // TODO 5.转换类型为json对象，按照mid进行聚合
        final KeyedStream<JSONObject, String> keyedBaseLogStream =
                validBaseLogStream
                .map(JSONObject::parseObject)
                .keyBy((data) -> data.getJSONObject("common").getString("mid"));

        // TODO 6.使用keyProcessFunction，使用valueState保存用户是否登入过,校验新旧用户
        final SingleOutputStreamOperator<JSONObject> fixedLogStream = keyedBaseLogStream
                .process(new checkNewUserFunction());

        // TODO 7.将不同类型的日志发送到不同的侧输出流中
        // 启动日志进入启动侧输出流中
        // 页面数据进入主流
        // 启动日志进入侧输出流
        final SingleOutputStreamOperator<String> pageStream = fixedLogStream
                .process(new SplitLogProcessFunction());
        // 获取曝光流
        final DataStream<String> displayStream = pageStream.getSideOutput(displayLogOT);
        // 获取开始流
        final DataStream<String> startStream = pageStream.getSideOutput(startLogOT);

        // TODO 处理多个输出流，sink到不同的kafka主题中
        startStream.addSink(KafkaUtil.getProducer(GmallConstants.DWD_START_LOG));
        displayStream.addSink(KafkaUtil.getProducer(GmallConstants.DWD_DISPLAY_LOG));
        pageStream.addSink(KafkaUtil.getProducer(GmallConstants.DWD_PAGE_LOG));

        // 打印测试
        // 打印脏数据的侧输出流
        validBaseLogStream.getSideOutput(dirtyLogOT).print("dirty_log>>>>>>>>>>>>>");
        startStream.print("dwd_start_log>>>>>>>>>>");
        displayStream.print("dwd_display_log>>>>>>>>>>");
        pageStream.print("dwd_page_log>>>>>>>>>");

        // TODO 8.执行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 校验用户是否首次登入,如果不是，且传来的is_new=1,则进行修正
     * 用RichMapFunction也可以
     */
    private static class checkNewUserFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {

        // 创建状态描述符,记录用户是否登入过(默认为false)
        private final ValueStateDescriptor<Boolean> firstLoginStateDesc
                = new ValueStateDescriptor<Boolean>(
                        "first-login-state",
                        BasicTypeInfo.BOOLEAN_TYPE_INFO
                );

        // 声明状态
        private ValueState<Boolean> firstLoginState;

        @Override
        public void open(Configuration parameters) throws Exception {

            firstLoginState = getRuntimeContext().getState(firstLoginStateDesc);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

            // 初始化状态
            Boolean isFirstLogin = firstLoginState.value();
            if (isFirstLogin == null) {
                isFirstLogin = true;
            }

            // 如果客户端生成的数据is_new=1,且在flink中为已登入过的状态，则进行修改
            if ("1".equals(value.getJSONObject("common").getString("is_new"))
                    && !isFirstLogin) {
                        value.getJSONObject("common")
                        .put("is_new", "0");
            }

            // 如果更新
            firstLoginState.update(false);

            out.collect(value);
        }
    }

    /**
     * 使用侧输出流将日志进行分流
     */
    private static class SplitLogProcessFunction
            extends ProcessFunction<JSONObject, String> {

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

            // TODO 判断启动流
            final String start = value.getString("start");
            if (start != null) { // 获取不存在的字段，返回值为null
                // 启动流加入到侧输出流中
                ctx.output(startLogOT, value.toJSONString());
            } else {
                // TODO 判断曝光流并进行炸裂
                final JSONArray displays = value.getJSONArray("displays");
                final String page_id = value.getJSONObject("page").getString("page_id");
                if (displays != null) {
                    // 遍历输出曝光的商品信息
                    for (int i = 0; i < displays.size(); i++) {
                        final JSONObject displayItem = displays.getJSONObject(i);
                        displayItem.put("page_id", page_id);
                        ctx.output(displayLogOT, displayItem.toJSONString());
                    }
                } else {
                    // TODO 其他页面日志传入主流
                    out.collect(value.toJSONString());
                }
            }

        }
    }
}
