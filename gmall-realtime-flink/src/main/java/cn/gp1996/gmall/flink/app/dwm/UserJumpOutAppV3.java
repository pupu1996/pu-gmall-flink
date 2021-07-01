package cn.gp1996.gmall.flink.app.dwm;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author  gp1996
 * @date    2021-06-30
 * @desc    使用状态实现判断用户跳出数据
 */
public class UserJumpOutAppV3 {

    public static void main(String[] args) {

        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.从数据源读取数据
        final SingleOutputStreamOperator<JSONObject> pageLogStream = env.addSource(KafkaUtil.getConsumer(
                GmallConstants.DWM_USER_JUMP_OUT_SOURCE_TOPIC,
                GmallConstants.DWM_USER_JUMP_OUT_GROUP_ID + "_2"
        )).map(JSON::parseObject);

        // TODO 3.设置水位线
        final WatermarkStrategy<JSONObject> wmStrategy = WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });
        final SingleOutputStreamOperator<JSONObject> pageLogStreamWithWm =
                pageLogStream.assignTimestampsAndWatermarks(wmStrategy);

        // TODO 4.使用状态识别用户跳出
    }
}
