package cn.gp1996.gmall.flink.app.dwm;

import cn.gp1996.gmall.flink.app.func.OrderWideAppFunc;
import cn.gp1996.gmall.flink.bean.OrderDetail;
import cn.gp1996.gmall.flink.bean.OrderInfo;
import cn.gp1996.gmall.flink.bean.OrderWide;
import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author  gp1996
 * @desc    2021-06-29
 * @desc    dwm层join出订单明细数据，便于dws层对其结果的复用
 */
public class OrderWideApp {

    public static void main(String[] args) {

        // TODO 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置CK
//        env.enableCheckpointing(GmallConstants.DEFAULT_CK_INTERNAL, CheckpointingMode.EXACTLY_ONCE);
//        // 设置状态后端
//        env.setStateBackend(new FsStateBackend(
//                GmallConstants.FLINK_APP_CK_PATH + "/" + OrderDetailApp.class.getSimpleName()
//        ));
//        // 设置checkpoint保留策略
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );

        // TODO 3.获取order_info,order_detail数据,并转换类型
        final SingleOutputStreamOperator<OrderInfo> orderInfoStream = env.addSource(KafkaUtil.getConsumer(
                GmallConstants.DWM_ORDER_INFO_SOURCE_TOPIC,
                GmallConstants.DWM_ORDER_WIDE_GROUP_ID
        )).map(new OrderWideAppFunc.ConvertOrderInfoMapFunction());

        final SingleOutputStreamOperator<OrderDetail> orderDetailStream = env.addSource(KafkaUtil.getConsumer(
                GmallConstants.DWM_ORDER_DETAIL_SOURCE_TOPIC,
                GmallConstants.DWM_ORDER_WIDE_GROUP_ID
        )).map(new OrderWideAppFunc.ConvertOrderDetailMapFunction());

        // TODO 4.设置事件时间戳
        final WatermarkStrategy<OrderInfo> wmStrategyForOrderInfo = WatermarkStrategy
                .<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<OrderInfo>)
                        (oi, recordTimestamp) -> oi.getCreate_ts());
        final WatermarkStrategy<OrderDetail> wmStrategyForOrderDetail = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<OrderDetail>) (od, recordTimestamp) -> od.getCreate_ts());
        final SingleOutputStreamOperator<OrderInfo> orderInfoStreamWithWm =
                orderInfoStream.assignTimestampsAndWatermarks(wmStrategyForOrderInfo);
        final SingleOutputStreamOperator<OrderDetail> orderDetailStreamWithWm =
                orderDetailStream.assignTimestampsAndWatermarks(wmStrategyForOrderDetail);

        // TODO 5.进行双流join keyBy -> Interval Join -> 范围join -> ProcessJoinFunction
        final SingleOutputStreamOperator<String> orderWideStream = orderInfoStreamWithWm.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailStreamWithWm.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-GmallConstants.CLUSTER_MAX_OUT_OF_ORDER_MS),
                        Time.seconds(GmallConstants.CLUSTER_MAX_OUT_OF_ORDER_MS))
                .process(new OrderWideAppFunc.JoinOrderInfoAndOrderDetailFunction())
                .map(data->JSONObject.toJSONString(data, SerializerFeature.WriteMapNullValue));


        // TODO 6.对order_Info和order_detail join的结果进行维度表join
        orderDetailStream.process()


        // TODO 7.sink到下游kafka
        // orderWideStream.addSink(KafkaUtil.getProducer(GmallConstants.DWM_ORDER_WIDE_SINK_TOPIC));
        // 测试输出
        orderWideStream.print("order-wide");

        // TODO 8.执行join
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
