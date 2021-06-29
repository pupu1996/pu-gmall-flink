package cn.gp1996.gmall.flink.app.dwm;

import cn.gp1996.gmall.flink.app.func.UniqueVisitFilterFunction;
import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author  gp1996
 * @date    2021-06-28
 * @desc    需求,以天为单位,对设备(mid)的访问进行过滤.只输出当天第一次访问的用户
 *          核心: 判断一个设备mid是否是当天第一次访问
 *          条件1: last_page_id是否为null,即是否是某次登入后第一个访问的页面
 *          条件2: 一个设备一天内会登入多次,如需要进行去重
 *          条件3: 当存在跨天访问时,需要判断是否是同一天访问的,如果第二天日期不同,记为第二天首次访问
 *
 *          数据源: dwd_page_log
 *          实现: 使用键控状态ValueState保存设备上次访问的时间
 *          目的: 按天进行过滤,将当天首次访问的mid传入下游(只是做了去重)
 *          DWS,DWM层的设计一定要关注需求，由需求决定去做什么
 *
 */
public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {

        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置checkpoint
//        env.enableCheckpointing(60 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE); // 1 hour
//        env.setStateBackend(new FsStateBackend(
//                GmallConstants.FLINK_APP_CK_PATH + "/" + UniqueVisitApp.class.getSimpleName()));
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 设置job的重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        // TODO 3.获取数据源,转换成JsonObject
        final SingleOutputStreamOperator<JSONObject> pageLogStream = env.addSource(KafkaUtil.getConsumer(
                GmallConstants.DWM_UV_SOURCE_TOPIC, GmallConstants.DWM_UV_GROUP_ID)
        ).map(JSONObject::parseObject);

        // TODO 4.***** 对设备进行过滤 ********
        // keyBy -> 使用ValueState
        final SingleOutputStreamOperator<String> DUVStream = pageLogStream
                .keyBy((data) -> data.getJSONObject("common").getString("mid"))
                .filter(new UniqueVisitFilterFunction())
                .map(data->JSONObject.toJSONString(data, SerializerFeature.WriteMapNullValue));

        // 测试输出
        DUVStream.print("uv");

        // TODO 5.sink到下游kafka
        DUVStream.addSink(KafkaUtil.getProducer(GmallConstants.DWM_UV_SINK_TOPIC));

        // TODO 6.执行
        env.execute();

        System.out.println(UniqueVisitApp.class.getSimpleName());

    }
}
