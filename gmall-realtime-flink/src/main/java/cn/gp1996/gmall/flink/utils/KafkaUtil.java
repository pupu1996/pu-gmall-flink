package cn.gp1996.gmall.flink.utils;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author  gp1996
 * @date    2021-06-25
 * @desc    kafka工具类
 */
public class KafkaUtil {

    private static Properties properties;

    static {
        properties = PropertyUtil.resolveProperties(GmallConstants.CONF_PATH_KAFKA);
    }

    private final static String DWD_DEFAULT_TOPIC = "dwd_default_topic";

    /**
     * ---------------------------------------------
     *                      Producer | sink
     * ---------------------------------------------
     */

    /**
     * 返回kafka Sink 对象
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(), // -> 将数据包装成ProducerRecord
                properties);
    }

    /**
     * 返回kafka Producer，使用自定义的序列化器
     * @param schema
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer getProducerWithCustomSchema(KafkaSerializationSchema<T> schema) {
        return new FlinkKafkaProducer<>(
                DWD_DEFAULT_TOPIC,
                schema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    /**
     * ---------------------------------------------
     *                      Consumer | source
     * ---------------------------------------------
     */

    /**
     * 返回kafka Source 对象
     * @param topic
     * @return
     */
    public static FlinkKafkaConsumer<String> getConsumer(String topic, String groupId) {

        final Properties consumerProps = new Properties(KafkaUtil.properties);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                KafkaUtil.properties
        );
    }

    // test
    public static void main(String[] args) {
        System.out.println(properties);
        final FlinkKafkaProducer<String> dfsf = getProducer("dfsf");
    }
}
