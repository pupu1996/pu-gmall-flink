package cn.gp1996.gmall.flink.app.dwm;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc    输出跳出的数据
 *          水线超过了第二条数据的时间才会认为第二次匹配上了,不然会认为中间可能还有其他数据到来,造成这次匹配失效
 *          注意：
 *          Pattern.begin("...").where(...).times(n).within(10s) => within是开集的,不包含超时时刻本身
 *          Pattern.begin("start").where(...)
 *              .next("second").where(...)
 *              .within(10s)  map{"start"}
 */
public class UserJumpOutApp {

    // 定义超时数据的ot
    private static final OutputTag<JSONObject> TIMEOUT_OT =
            new OutputTag<>(
                    "user-jump-out-timeout-ot",
                    TypeInformation.of(JSONObject.class));

    public static void main(String[] args) throws Exception {
        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.获取数据源
        final SingleOutputStreamOperator<JSONObject> pageLogStream = env.addSource(KafkaUtil.getConsumer(
                GmallConstants.DWM_USER_JUMP_OUT_SOURCE_TOPIC,
                GmallConstants.DWM_USER_JUMP_OUT_GROUP_ID + "1"
        )).map(JSONObject::parseObject);

        // TODO 3.设置水线
        final WatermarkStrategy<JSONObject> wmStrategy =
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject pageLog, long recordTimestamp) {
                                return pageLog.getLong("ts");
                            }
                        }
                );
        final SingleOutputStreamOperator<JSONObject> pageLogStreamWithWm =
                pageLogStream.assignTimestampsAndWatermarks(wmStrategy);

        // TODO 4.按照设备id key by
        final KeyedStream<JSONObject, String> keyedPageLogStream =
                pageLogStreamWithWm.keyBy((data) -> (data.getJSONObject("common").getString("mid")));

        // TODO 5.定义模式
        final Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(10)); // within作用于中间数据的间隔

        // TODO 6.将模式应用于流
        final PatternStream<JSONObject> patternStream = CEP.pattern(keyedPageLogStream, pattern);

        // TODO 7.过滤出匹配的事件(将匹配上的第一条访问数据和超时的第一条数据输出)
        final SingleOutputStreamOperator<JSONObject> userJumpOutMatchStream = patternStream.select(
                TIMEOUT_OT,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                }
        );

        final DataStream<JSONObject> userJumpOutTimeoutStream =
                userJumpOutMatchStream.getSideOutput(TIMEOUT_OT);

        // TODO 8.将两条流进行union(union可以合并多条流，要求流的类型是相等的)
        final SingleOutputStreamOperator<String> userJumpOutStream =
                userJumpOutMatchStream.union(userJumpOutTimeoutStream)
                .map(data->JSONObject.toJSONString(data, SerializerFeature.WriteMapNullValue));

        // TODO 9.输出数据到kafka
        userJumpOutStream.addSink(KafkaUtil.getProducer(
                GmallConstants.DWM_USER_JUMP_OUT_SINK_TOPIC
        ));

        // 测试输出
        userJumpOutStream.print("jump-out");

        // TODO 10.执行任务
        env.execute();
    }
}
