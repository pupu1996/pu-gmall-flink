package cn.gp1996.gmall.flink.constants;

/**
 * @author  gp1996
 * @date    2021-06-25
 * @desc   gmall_flink常量定义
 */
public class GmallConstants {

    // -------------------------------------------------
    // 配置文件路径
    // -------------------------------------------------
    public final static String CONF_PATH_GMALL = "gmall.properties";
    public final static String CONF_PATH_KAFKA = "kafka.properties";
    public final static String CONF_PATH_DRUID = "druid.properties";

    // -------------------------------------------------
    // Flink App公共配置
    // -------------------------------------------------
    public final static String FLINK_APP_CK_PATH = "hdfs://hadoop111:8020/gmall-flink/ck/";
    public final static Long DEFAULT_CK_INTERNAL = 60 * 60 * 1000L;
    public final static Long CLUSTER_MAX_OUT_OF_ORDER_MS = 5L;

    // -------------------------------------------------
    // Redis连接配置
    // -------------------------------------------------
    public final static String REDIS_SERVER = "hadoop111";
    public final static int REDIS_PORT = 6379;

    // -------------------------------------------------
    // Druid连接配置
    // -------------------------------------------------

    // -------------------------------------------------
    // ods 相关配置
    // -------------------------------------------------

    public final static String ODS_BASE_DB = "ods_base_db";
    public final static String ODS_BASE_DB_TEST = "ods_base_db_test";
    public final static String ODS_BASE_LOG = "ods_base_log";
    public final static String ODS_BASE_LOG_TEST = "ods_base_log_test";

    // -------------------------------------------------
    // dwd 相关配置
    // -------------------------------------------------
    // 消费者组
    // app:BaseLogApp kafka source的groupId
    public final static String GROUP_BASE_LOG_APP_TEST = "group-base-log-app-test";
    public final static String GROUP_BASE_LOG_APP = "group-base-log-app";
    // app:BaseDBApp kafka source的groupId
    public final static String GROUP_BASE_DB_APP_TEST = "group-base-db-app-test";
    public final static String GROUP_BASE_DB_APP = "group-base-db-app";
    // app:BaseDBApp 维度表侧输出流的outputTag
    public final static String DIM_OUTPUT = "dim_ot";

    // 侧输出流名称
    // app:BaseLogApp 脏数据side output tag
    public final static String OT_ERROR_BASE_LOG_APP = "base-log-app-error-ot";
    // app:BaseLogApp 启动流tag
    public final static String OT_START_LOG = "dwd-start-log-ot";
    // app:BaseLogApp 曝光流tag
    public final static String OT_DISPLAY_LOG = "dwd-display-log-ot";

    // 维度表主题
    public final static String DIM_ACTIVITY_INFO = "dim_activity_info";
    public final static String DIM_ACTIVITY_RULE = "dim_activity_rule";
    public final static String DIM_ACTIVITY_SKU = "dim_activity_sku";
    public final static String DIM_BASE_CATEGORY1 = "dim_base_category1";
    public final static String DIM_BASE_CATEGORY2 = "dim_base_category2";
    public final static String DIM_BASE_CATEGORY3 = "dim_base_category3";
    public final static String DIM_BASE_DIC = "dim_base_dic";
    public final static String DIM_BASE_PROVINCE = "dim_base_province";
    public final static String DIM_BASE_REGION = "dim_base_region";
    public final static String DIM_BASE_TRADEMARK = "dim_base_trademark";
    public final static String DIM_COUPON_INFO = "dim_coupon_info";
    public final static String DIM_COUPON_RANGE = "dim_coupon_range";
    public final static String DIM_FINANCIAL_SKU_COST = "dim_financial_sku_cost";
    public final static String DIM_SKU_INFO = "dim_sku_info";
    public final static String DIM_SPU_INFO = "dim_spu_info";
    public final static String DIM_USER_INFO = "dim_user_info";


    // 事实表主题
    // ----------------- 用户日志 ---------------------
    public final static String DWD_START_LOG = "dwd_start_log";
    public final static String DWD_START_LOG_TEST = "dwd_start_log_test";
    public final static String DWD_DISPLAY_LOG = "dwd_display_log";
    public final static String DWD_DISPLAY_LOG_TEST = "dwd_display_log_test";
    public final static String DWD_PAGE_LOG = "dwd_page_log";
    public final static String DWD_PAGE_LOG_TEST = "dwd_page_log_test";
    // ----------------- 业务 ---------------------
    public final static String DWD_ORDER_INFO_TOPIC = "dwd_order_info";
    public final static String DWD_ORDER_DETAIL_TOPIC = "dwd_order_detail";

    // -------------------------------------------------
    // dwm 相关配置
    // -------------------------------------------------
    // App: UniqueVisitApp
    // 数据来源
    public final static String DWM_UV_SOURCE_TOPIC = DWD_PAGE_LOG;
    // 目标主题
    public final static String DWM_UV_SINK_TOPIC = "dwm_unique_visit";
    // 消费者组id
    public final static String DWM_UV_GROUP_ID = "group-dwm-uv-app";
    // uv状态名称
    public final static String DWM_UV_STATE_NAME = "uv-state";

    // App: UserJumpOutApp 求跳出行为发生的数据
    // 数据来源
    public final static String DWM_USER_JUMP_OUT_SOURCE_TOPIC = DWD_PAGE_LOG;
    public final static String DWM_USER_JUMP_OUT_GROUP_ID = "group-dwm-user-jump-out-app";
    public final static String DWM_USER_JUMP_OUT_SINK_TOPIC = "dwm_user_jump_out";

    // App: OrderWideApp
    // 数据来源
    // order_detail
    public final static String DWM_ORDER_DETAIL_SOURCE_TOPIC = DWD_ORDER_DETAIL_TOPIC;
    public final static String DWM_ORDER_INFO_SOURCE_TOPIC = DWD_ORDER_INFO_TOPIC;
    public final static String DWM_ORDER_WIDE_GROUP_ID = "group-dwm-order-wide";
    // dwn层订单宽表主题
    public final static String DWM_ORDER_WIDE_SINK_TOPIC = "dwm_order_wide";
}