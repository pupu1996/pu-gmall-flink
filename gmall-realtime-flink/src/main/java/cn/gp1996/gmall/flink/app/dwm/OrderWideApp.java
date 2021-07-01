package cn.gp1996.gmall.flink.app.dwm;

import cn.gp1996.gmall.flink.app.func.AsyncJoinFunction;
import cn.gp1996.gmall.flink.app.func.DimJoinFunctions;
import cn.gp1996.gmall.flink.app.func.GenericAsyncJoinFunction;
import cn.gp1996.gmall.flink.app.func.OrderWideAppFunc;
import cn.gp1996.gmall.flink.bean.OrderDetail;
import cn.gp1996.gmall.flink.bean.OrderInfo;
import cn.gp1996.gmall.flink.bean.OrderWide;
import cn.gp1996.gmall.flink.constants.GmallConstants;
import cn.gp1996.gmall.flink.utils.KafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

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
        final SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoStreamWithWm.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailStreamWithWm.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-GmallConstants.CLUSTER_MAX_OUT_OF_ORDER_MS),
                        Time.seconds(GmallConstants.CLUSTER_MAX_OUT_OF_ORDER_MS))
                .process(new OrderWideAppFunc.JoinOrderInfoAndOrderDetailFunction());
        orderWideStream.print("Order-wide>>>>>>>>>>>");

        // TODO 6.对order_Info和order_detail join双流join的结果进行维度表join
        // TODO 在后续的每个算子中都要建立连接
        // TODO 6.1 join用户表
        final SingleOutputStreamOperator<OrderWide> userWideStream = AsyncDataStream.unorderedWait(
                orderWideStream,
                new DimJoinFunctions.JoinDimUserInfo(GmallConstants.DIM_USER_INFO),
                80,
                TimeUnit.SECONDS,
                100
        );
        // TODO 6.2 join省份表
        final SingleOutputStreamOperator<OrderWide> provinceWideStream = AsyncDataStream.unorderedWait(
                userWideStream,
                new DimJoinFunctions.JoinDimBaseProvince(GmallConstants.DIM_BASE_PROVINCE),
                80,
                TimeUnit.SECONDS,
                100
        );
        // provinceWideStream.print("provinceWide>>>>>>>>>>>");

        // TODO 6.3 join sku_info表
        final SingleOutputStreamOperator<OrderWide> skuWideStream = AsyncDataStream.unorderedWait(
                provinceWideStream,
                new DimJoinFunctions.JoinDimSkuInfo(GmallConstants.DIM_SKU_INFO),
                80,
                TimeUnit.SECONDS,
                100
        );
        // skuWideStream.print("skuWide>>>>>>>>>>>");

        // TODO 6.4 根据 tm_id join 品牌表
        final SingleOutputStreamOperator<OrderWide> trademarkWideStream = AsyncDataStream.unorderedWait(
                skuWideStream,
                new DimJoinFunctions.JoinDimBaseTradeMark(GmallConstants.DIM_BASE_TRADEMARK),
                80,
                TimeUnit.SECONDS,
                100
        );

        // TODO 6.5 根据 category3_id join 分类表
        final SingleOutputStreamOperator<OrderWide> category3WideStream = AsyncDataStream.unorderedWait(
                trademarkWideStream,
                new DimJoinFunctions.JoinDimBaseCategory3(GmallConstants.DIM_BASE_CATEGORY3),
                80,
                TimeUnit.SECONDS,
                100
        );

        // TODO 6.6 根据 spu_id join spu_info表
        final SingleOutputStreamOperator<OrderWide> spuWideStream = AsyncDataStream.unorderedWait(
                category3WideStream,
                new DimJoinFunctions.JoinDimSpuInfo(GmallConstants.DIM_SPU_INFO),
                80,
                TimeUnit.SECONDS,
                100
        );

        spuWideStream.print("spuWideStream>>>>>>>>");

        // TODO 7.sink到下游kafka
        // orderWideStream.addSink(KafkaUtil.getProducer(GmallConstants.DWM_ORDER_WIDE_SINK_TOPIC));
        // 测试输出

        // TODO 8.执行join
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
