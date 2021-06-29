package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * @author  gp1996
 * @date    2021-06-028
 * @desc    以天为单位,过滤出当天首次访问的用户
 */
public class UniqueVisitFilterFunction extends RichFilterFunction<JSONObject> {

    // 时间格式转换器
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    // 定义uv状态描述符
    private final ValueStateDescriptor<String> uvStateDesc =
            new ValueStateDescriptor<>(GmallConstants.DWM_UV_STATE_NAME, String.class);

    // 声明uv状态
    private transient ValueState<String> uvState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取状态
        uvStateDesc.enableTimeToLive(
                StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // 为当前键的状态设置ttl,对于长久未访问的设备状态进行清除,防止状态数据量过大
                .build());
        // 保存当前mid上次当日首次访问的时间
        uvState = getRuntimeContext().getState(uvStateDesc);
    }

    @Override
    public boolean filter(JSONObject value) throws Exception {
        /**
         * 判断逻辑:
         * (1) 过滤出last_page_id = null的数据 => 设备每次登入访问的首个页面
         * (2) if uvState == null(当天未访问过) || value.ts=>date.equals(uvState.value())
         *                                         (处理跨天访问,如果状态中的日期和当前登入日期不同,算是第二天的首次登入)
         *    // 更新状态
         *    // 输出数据到下游
         * else
         *   // 直接过滤
         *
         */

        //获取last_page_id
        final String lastPageId = value.getJSONObject("page").getString("last_page_id");
        // 保留last_page_id为null的数据
        if (lastPageId != null) {
            return false;
        }

        // 获取状态中该设备的上次登入日期
        final String lastVisitDate = uvState.value();
        // 获取当前数据的登入时间
        final String curLogDate = sdf.format(new Date(value.getLong("ts")));
        if (lastVisitDate == null || !curLogDate.equals(lastVisitDate)) {
            // 更新最后访问的时间
            uvState.update(curLogDate);
            // 输出数据
            return true;
        } else {
            return false;
        }

    }
}
