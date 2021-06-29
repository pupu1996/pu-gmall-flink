package cn.gp1996.gmall.flink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Locale;

/**
 * @author  gp1996
 * @date    2021-06-25
 * @desc    自定义Flink-CDC gmall业务数据反序列化器
 */
public class CDC_DB_DeserializationSchema
        implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        // TODO 获取主题信息(数据库名+表名)
        final String topic = sourceRecord.topic();
        final String[] db_tbl = topic.split("\\.");
        String db = db_tbl[1];
        String tbl = db_tbl[2];

        // TODO 获取操作
        // 四种操作类型 CREATE|UPDATE|DELETE|READ
        final Envelope.Operation op = Envelope.operationFor(sourceRecord);

        // TODO 获取每行数据(不同操作value结构体中保存的信息不同)
        // create | after
        // update | before、after
        // delete | before
        final Struct value =
                (Struct) sourceRecord.value();
        // 获取value Struct中的after
        final Struct after =  value.getStruct("after");
        // 获取value Struct中的before
        final Struct before = value.getStruct("before");

        // TODO 判断after|before的情况
        // 保存结果类型
        // final LinkedList<JSONObject> jsonObjs = new LinkedList<>();
        JSONObject afterObj = new JSONObject();
        JSONObject beforeObj = new JSONObject();

        if (after != null) {
            final List<Field> fields = after.schema().fields();
            for (Field field : fields) {
                final Object o = after.get(field);
                afterObj.put(field.name(), o);
            }
            // jsonObjs.add(afterObject);

        }

        if (before != null) {
            final List<Field> fields = before.schema().fields();
            for (Field field : fields) {
                final Object o = before.get(field);
                beforeObj.put(field.name(), o);
            }
            // jsonObjs.add(beforeObject);

        }

        // delete的情况
//        if (before != null && after == null) {
//            afterObj = beforeObj;
//            beforeObj = new JSONObject();
//        }

        // TODO 输出结果
        final String resLog = concatRes(
                op,
                JSON.toJSONString(afterObj, SerializerFeature.WriteMapNullValue),
                JSON.toJSONString(beforeObj, SerializerFeature.WriteMapNullValue),
                db,
                tbl
        );
        collector.collect(resLog);

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    // TODO 拼接最终的对象
    private String concatRes(Envelope.Operation op, String after, String before, String database, String table) {
        final JSONObject resStr = new JSONObject();
        resStr.put("type", "create".equals(op.name().toLowerCase(Locale.ROOT)) ? "insert" : op.name().toLowerCase(Locale.ROOT));
        resStr.put("data", after);
        resStr.put("before", before);
        resStr.put("database", database);
        resStr.put("tableName", table);

        return JSON.toJSONString(resStr, SerializerFeature.WriteMapNullValue);
    }
}
