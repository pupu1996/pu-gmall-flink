package cn.gp1996.gmall.flink.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author  gp1996
 * @date    2021-06-27
 * @desc    自定义Kafka序列化schema，将经过分流的事实表数据sink到事实表的各自主题中
 */
public class DwdFactSerializationSchema implements KafkaSerializationSchema<JSONObject> {

    // {"sinkTable":"dim_trademark","data":{"tm_name":"安踏4","id":20},"sinkColumns":"id,tm_name"}
    @Override
    public ProducerRecord<byte[], byte[]> serialize(JSONObject value, @Nullable Long ts) {

        // 获取目标主题
        final String sinkTopic = value.getString("sinkTable");

        return new ProducerRecord<>(
                sinkTopic,
                JSONObject.toJSONBytes(value.getJSONObject("data"), SerializerFeature.WriteMapNullValue)
        );
    }
}
