package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.constants.PhoenixConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class PhoenixSinkFunction extends RichSinkFunction<JSONObject> {

    // 保存jdbc phoenix连接对象
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {

        // 加载驱动类;
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        //  配置
        final Properties phoenixProps = new Properties();
        phoenixProps.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");

        // 获取jdbc连接
        conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER, phoenixProps);
    }

    // 处理到来的每条数据
    // kafka> {"tm_name":"安踏","sinkTable":"dim_trademark","logo_url":"/static1/default.jpg","id":12}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement pst = null;
        try {
            String dml = concatDML(value);
            System.out.println(dml);
            pst = conn.prepareStatement(dml);
            pst.execute();
            conn.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (pst != null) {
                pst.close();
            }
        }
    }

    /**
     * 拼接dml语句，插入和更新数据
     * upsert into table dwd_base_trademark(id,tm_name) values(12, ’安踏‘)
     * @param value
     * @return
     */
    private String concatDML(JSONObject value) {
        Preconditions.checkNotNull(value);
        // 1.获取目标表名
        final String sinkTable = value.getString("sinkTable");
        // 2.获取需要的列
        final String sinkColumns = value.getString("sinkColumns");
        final String[] sinkColumnsArr = sinkColumns.split(",");
        // 3.拼接插入语句
        final StringBuilder upsert =
                new StringBuilder("upsert into ")
                .append(PhoenixConfig.HBASE_SCHEMA).append(".").append(sinkTable)
                .append("(").append(sinkColumns).append(")")
                .append(" values(");
        for (int i = 0; i < sinkColumnsArr.length; i++) {
            final String fieldName = sinkColumnsArr[i];
            upsert.append("'").append(value.getJSONObject("data").getString(fieldName)).append("'")
                    .append(i < sinkColumnsArr.length - 1 ? "," : "");
        }

        upsert.append(")");
        return upsert.toString();
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
