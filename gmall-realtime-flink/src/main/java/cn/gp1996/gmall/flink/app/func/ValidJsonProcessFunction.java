package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ValidJsonProcessFunction extends ProcessFunction<String, String> {

    // 脏数据是否需要进入侧输出流
    private Boolean validSideOut = true;
    // 侧输出流Tag的名称
    private String outTagName = "error-json-parse";

    public ValidJsonProcessFunction(
            Boolean validSideOut,
            String outTagName
    ) {
        this.validSideOut = validSideOut;
        if (outTagName != null) {
            this.outTagName = outTagName;
        }
    }


    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

        // 尝试装换成json
        try {
            final JSONObject baseLogObj = JSONObject.parseObject(value);
            out.collect(baseLogObj.toJSONString());
        } catch (Exception e) {
            if (validSideOut) {
                ctx.output(new OutputTag<>(GmallConstants.OT_ERROR_BASE_LOG_APP, BasicTypeInfo.STRING_TYPE_INFO), value);
            }

        }
//        final JSONObject baseLogObj = JSONObject.parseObject(value);
//        out.collect(baseLogObj.toJSONString());
    }

    public static void main(String[] args) {
        String test = "{\"common\":{\"ar\":\"110000\",\"ba\":\"Huawei\",\"ch\":\"wandoujia\",\"is_new\":\"1\",\"md\":\"Huawei P30\",\"mid\":\"mid_5\",\"os\":\"Android 11.0\",\"uid\":\"40\",\"vc\":\"v2.1.134\"},\"displays\":[{\"display_type\":\"query\",\"item\":\"6\",\"item_type\":\"sku_id\",\"order\":1,\"pos_id\":5},{\"display_type\":\"query\",\"item\":\"6\",\"item_type\":\"sku_id\",\"order\":2,\"pos_id\":1},{\"display_type\":\"promotion\",\"item\":\"7\",\"item_type\":\"sku_id\",\"order\":3,\"pos_id\":2},{\"display_type\":\"query\",\"item\":\"8\",\"item_type\":\"sku_id\",\"order\":4,\"pos_id\":5},{\"display_type\":\"query\",\"item\":\"5\",\"item_type\":\"sku_id\",\"order\":5,\"pos_id\":4}],\"page\":{\"during_time\":12047,\"item\":\"苹果手机\",\"item_type\":\"keyword\",\"last_page_id\":\"search\",\"page_id\":\"good_list\"},\"ts\":1608276764000}";
        final JSONObject jsonObject = JSONObject.parseObject(test);
        System.out.println(jsonObject.getJSONObject("common").getString("mid"));
    }
}
