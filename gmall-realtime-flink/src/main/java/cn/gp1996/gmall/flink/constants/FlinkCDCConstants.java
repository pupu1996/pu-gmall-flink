package cn.gp1996.gmall.flink.constants;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc    FlinkCDC配置
 */
public class FlinkCDCConstants {

    // --------- 基本连接属性 -----------
    // 业务数据库所在主机名
    public static String CDC_CONN_HOST_NAME = "hadoop111";

    public static int CDC_CONN_PORT = 3306;

    public static String CDC_CONN_USER_NAME = "root";

    public static String CDC_CONN_PWD = "qqfdjkd12";


    // -------- 自定义反序列化json字段 ---------
    public final static String DATABASE = "database";
    public final static String TABLE_NAME = "tableName";
    public final static String TYPE = "type";
    public final static String DATA = "data";
    public final static String BEFORE = "before";


    // -------- app: ODS_DB_CDC同步的数据库列表 ---------
    public static String WATCHED_BASE_DB = "gmall_flink";

    // -------- app: TableProcessCDC同步的数据库列表 --------
    public static String WATCHED_TABLE_PROCESS_DB = "gmall_flink_extend";

    public static String WATCHED_TABLE_PROCESS = "gmall_flink_extend.table_process";

    public static String TABLE_PROCESS_SINK_TOPIC = "table_process_sink";
}
