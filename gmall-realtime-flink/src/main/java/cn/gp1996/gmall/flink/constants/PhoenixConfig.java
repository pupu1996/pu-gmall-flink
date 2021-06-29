package cn.gp1996.gmall.flink.constants;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc    phoenix连接配置
 *
 */
public class PhoenixConfig {

    // dim层,Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_FLINK_DIM";

    // Phoenix的驱动
    public static final String PHOENIX_DRIVER =
            "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER =
            "jdbc:phoenix:hadoop111,hadoop112,hadoop113:2181";

    // 允许用户操作schema
    public static final String IS_NAMESPACE_MAPPING_ENABLED =
            "phoenix.schema.isNamespaceMappingEnabled";

}
