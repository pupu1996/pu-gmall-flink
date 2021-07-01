package cn.gp1996.gmall.flink.utils;

import cn.gp1996.gmall.flink.constants.PhoenixConfig;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.*;
import java.util.Properties;

/**
 * @desc Druid连接池工具类 -> 连接Phoenix
 */
public class DruidUtil {

    // 保存druid连接池单例
    private static DruidDataSource dataSource;

    private static final int initSize = 10;
    private static final int maxActive = 20;
    private static final int minIdle = 10;
    private static final int maxWait = 5 * 10000;

    private static final boolean removeAbandoned = true;
    private static final int abandonedTimeOut = 600;

    private static final boolean testOnBorrow = false;
    private static final String validationQuery = "select 1";

    /**
     * 可供调用的初始化方法(传入连接配置？)
     */
    public static void init() throws SQLException {
        if (dataSource == null) {
            synchronized (DruidUtil.class) {
                if (dataSource == null) {
                    doInit();
                }
            }
        }
    }

    /**
     * 真正初始化连接池
     */
    private static void doInit() throws SQLException {
        System.out.println("------------初始化Druid连接池-------------");
        dataSource = new DruidDataSource();

        // 设置连接池参数
        dataSource.setInitialSize(initSize);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxActive(maxActive);
        dataSource.setMaxWait(maxWait);

        // 设置超时是否回收
        dataSource.setRemoveAbandoned(removeAbandoned);
        dataSource.setRemoveAbandonedTimeout(abandonedTimeOut);

        // 设置回收检测
        dataSource.setTestOnBorrow(testOnBorrow);
        dataSource.setValidationQuery(validationQuery);

        // 设置Phoenix连接属性
        // 创建配置
        final Properties properties = new Properties();
        properties.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");
        dataSource.setConnectProperties(properties);
        dataSource.setDriverClassName(PhoenixConfig.PHOENIX_DRIVER);
        dataSource.setUrl(PhoenixConfig.PHOENIX_SERVER);
        // 开启prepareStatement缓存(会占用JVM内存)
        dataSource.setPoolPreparedStatements(true);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(10);

        // real初始化连接池
        dataSource.init();
    }

    /**
     * 从Druid连接池中获取连接
     */
    public static Connection getConnection() throws SQLException {

        final DruidPooledConnection connection = dataSource.getConnection();
        // connection.setSchema(PhoenixConfig.HBASE_SCHEMA);

        return connection;
    }

    public static void main(String[] args) throws SQLException {
        // 1.测试Druid工具类
        DruidUtil.init();
        final Connection connection = getConnection();
        final PreparedStatement pst =
                connection.prepareStatement("select * from " + PhoenixConfig.HBASE_SCHEMA + ".dim_base_province");
        final ResultSet resultSet = pst.executeQuery();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        System.out.println("col count = " + metaData.getColumnCount());

        resultSet.close();
        pst.close();
        connection.close();
    }
}
