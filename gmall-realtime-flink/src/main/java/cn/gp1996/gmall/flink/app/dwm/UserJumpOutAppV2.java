package cn.gp1996.gmall.flink.app.dwm;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

/**
 * @author  gp1996
 * @desc    使用会话窗口实现匹配用户跳出行为(用户在一个会话中进入一个页面后,立马退出的行为)
 *          前题：如果数据中未携带会话id,则无法精确的区分会话id，只能通过时间大致判断(设置一个会话超时)
 *          (1) 按照用户进行keyBy
 *          (2) 使用sessionWindow设置Gap时间，大致区分会话(精确的会话可能更长或更短)
 *          (3) 对于每个会话窗口触发时,遍历窗口中的数据:
 *              情况1(不需要了,属于情况3)：如果窗口中只有一个元素，且last_page_id=null，即为超时数据，输出
 *              情况2：遍历窗口中的元素，两两对比，如果两个的last_page_id为null,输出第一个，
 *              用一个对象保存上一条数据即可
 *              情况(3): 最后一条数据的last_page_id为null,认为超时输出
 *
 *          延时较大，需要等到触发一个session窗口计算时,才输出结果
 *
 */
public class UserJumpOutAppV2 {
    public static void main(String[] args) throws Exception {

        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置ck

        // TODO 3.从数据源获取数据dwd_page_log
        final SingleOutputStreamOperator<JSONObject> pageLogStream = env.addSource(KafkaUtil.getConsumer(
                GmallConstants.DWM_USER_JUMP_OUT_SOURCE_TOPIC,
                GmallConstants.DWM_USER_JUMP_OUT_GROUP_ID
        )).map(JSONObject::parseObject);

        pageLogStream.print();

//        pageLogStream.print();
//        final Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop111:9092,hadoop112:9092");
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GmallConstants.DWM_USER_JUMP_OUT_GROUP_ID);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        final FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(
//                GmallConstants.DWM_USER_JUMP_OUT_SOURCE_TOPIC,
//                new SimpleStringSchema(),
//                properties
//        );

//        final DataStreamSource<String> sourceStream = env.addSource(stringFlinkKafkaConsumer);
//        sourceStream.print();

//        // TODO 4.设置水线策略(考虑乱序)
//        final WatermarkStrategy<JSONObject> wmStrategy = WatermarkStrategy
//                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
//                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//                    @Override
//                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                        return element.getLong("ts");
//                    }
//                });
//        final SingleOutputStreamOperator<JSONObject> pageLogStreamWithWm =
//                pageLogStream.assignTimestampsAndWatermarks(wmStrategy);
//
//        // TODO 5.keyBy -> 开窗 —> ProcessWindowFunction
//        pageLogStreamWithWm
//                .keyBy(data->data.getJSONObject("common").getString("mid"))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .process(new GetJumpOutDataFunction())
//                .print("jump-out");

        // TODO 6.执行job
        env.execute();

    }

    private static class GetJumpOutDataFunction
            extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
            // 窗口中的数据不会根据事件时间自动排序
            final Iterator<JSONObject> iter = elements.iterator();
            // 排序
            final ArrayList<JSONObject> logs = Lists.newArrayList(iter);
            logs.sort(new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return o1.getLong("ts").compareTo(o2.getLong("ts"));
                }
            });
            // 保存历史数据
            JSONObject lastLog = null;
            // 对比
            final Iterator<JSONObject> sortedLogIter = logs.iterator();
            while (sortedLogIter.hasNext()) {
                if (lastLog == null) {
                    lastLog = sortedLogIter.next();
                    continue;
                }

                // 对比上一条和这一条
                final JSONObject curLog = sortedLogIter.next();
                if (lastLog.getJSONObject("page").getString("last_page_id") == null &&
                        curLog.getJSONObject("page").getString("last_page_id") == null) {
                    out.collect(lastLog);
                }

                // 当前数据转为历史数据
                lastLog = curLog;

                // 判断最后一条
                if (!sortedLogIter.hasNext() &&
                        lastLog.getJSONObject("page").getString("last_page_id") == null) {
                    out.collect(curLog);
                }

            }

        }
    }
}
