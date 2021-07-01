package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.bean.OrderDetail;
import cn.gp1996.gmall.flink.bean.OrderInfo;
import cn.gp1996.gmall.flink.bean.OrderWide;
import cn.gp1996.gmall.flink.utils.DateUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author  gp1996
 * @date    2021-06-29
 * @desc    App: OrderDetailApp的自定义Function
 */
public class OrderWideAppFunc {

    /**
     * 转换jsonStr转换成orderInfo对象
     */
    public static class ConvertOrderInfoMapFunction
            implements MapFunction<String, OrderInfo> {

        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public OrderInfo map(String value) throws Exception {
            // value是json数据
            // 定义返回结果对象
            final OrderInfo orderInfoObj = JSONObject.parseObject(value, OrderInfo.class);

            // 解析日期、添加字段
            final String create_time = orderInfoObj.getCreate_time(); // DateTime时间戳类型
            final String[] dateTimeArr = create_time.split(" ");
            orderInfoObj.setCreate_date(dateTimeArr[0]);
            orderInfoObj.setCreate_hour(dateTimeArr[1].split(":")[0]);
            final Date dateTimeObj = sdf.parse(create_time);
            orderInfoObj.setCreate_ts(dateTimeObj.getTime());

            return  orderInfoObj;
        }

    }

    /**
     * 将数据jsonStr转换成OrderDetail对象
     */
    public static class ConvertOrderDetailMapFunction
            implements MapFunction<String, OrderDetail> {

        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public OrderDetail map(String value) throws Exception {
            // 1.创建返回结果对象
            final OrderDetail orderDetailObj = JSONObject.parseObject(value, OrderDetail.class);

            // 2.解析日期、添加字段
            final String dateTimeStr = orderDetailObj.getCreate_time();
            // System.out.println("Od dataTimeStr: " + dateTimeStr);
            final Date dateTimeObj = sdf.parse(dateTimeStr);
            orderDetailObj.setCreate_ts(dateTimeObj.getTime());

            return orderDetailObj;
        }
    }

    /**
     * 处理order_info，order——detail双流join
     */
    public static class JoinOrderInfoAndOrderDetailFunction
                extends ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide> {
        @Override
        public void processElement(OrderInfo oi, OrderDetail od, Context ctx, Collector<OrderWide> out) throws Exception {
            out.collect(new OrderWide(oi, od));
        }
    }
}
