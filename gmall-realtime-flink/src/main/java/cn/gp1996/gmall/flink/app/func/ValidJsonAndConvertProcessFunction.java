package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc
 * (1) 判断json能否成功解析
 * (2) 是否输入到侧输出流
 * (3) 转换成JsonObject
 */
public class ValidJsonAndConvertProcessFunction
            extends ProcessFunction<String, JSONObject> {

    private static final String OT_ERROR_JSON_PARSE = "error-json-parse";

    // 脏数据是否需要进入侧输出流
    private Boolean validSideOut = true;
    // 侧输出流OT
    private OutputTag<String> ot;
    // 默认侧输出流OT
    public static final OutputTag<String> DEFAULT_PARSE_ERR_OT =
        new OutputTag<>(
            OT_ERROR_JSON_PARSE,
            BasicTypeInfo.STRING_TYPE_INFO
    );

    public ValidJsonAndConvertProcessFunction (
            Boolean validSideOut
    ) {
        this(validSideOut, null);
    }

    public ValidJsonAndConvertProcessFunction (
            Boolean validSideOut,
            String outTagName
    ) {
        this.validSideOut = validSideOut;
        if (outTagName != null) {
            this.ot =  new OutputTag<>(
                    outTagName,
                    BasicTypeInfo.STRING_TYPE_INFO
            );
        } else {
            this.ot = DEFAULT_PARSE_ERR_OT;
        }
    }

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 尝试装换成json
        try {
            final com.alibaba.fastjson.JSONObject baseLogObj = com.alibaba.fastjson.JSONObject.parseObject(value);
            out.collect(baseLogObj);
        } catch (Exception e) {
            if (validSideOut) {
                ctx.output(new OutputTag<>(GmallConstants.OT_ERROR_BASE_LOG_APP, BasicTypeInfo.STRING_TYPE_INFO), value);
            }

        }
    }
}
