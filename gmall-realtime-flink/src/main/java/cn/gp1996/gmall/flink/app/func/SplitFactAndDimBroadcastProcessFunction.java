package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.bean.TableProcess;
import cn.gp1996.gmall.flink.constants.FlinkCDCConstants;
import cn.gp1996.gmall.flink.constants.PhoenixConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc    将配置表的广播数据和业务数据进行配置，
 *          根据配置表动态的去管理表的配置，可以在不停止应用的情况下，修改表流动的行为
 *
 */
public class SplitFactAndDimBroadcastProcessFunction
        extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 配置表状态描述符
    private MapStateDescriptor<String, TableProcess> tpStateDesc;

    // 维度表侧输出流的outputTag
    private OutputTag<JSONObject> dimOt;

    // 定义Phoenix连接
    transient private Connection conn;

    public SplitFactAndDimBroadcastProcessFunction(
            MapStateDescriptor<String, TableProcess> tpStateDesc,
            OutputTag<JSONObject> dimOt
    ) {
        this.tpStateDesc = tpStateDesc;
        this.dimOt = dimOt;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 打开Phoenix客户端(瘦)
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        final Properties phoenixConf = new Properties();
        phoenixConf.setProperty(PhoenixConfig.IS_NAMESPACE_MAPPING_ENABLED, "true");
        conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER, phoenixConf);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        // TODO 1.读取配置表状态(mapstate)，通过主键获取(只读)
        final ReadOnlyBroadcastState<String, TableProcess> tpState =
                ctx.getBroadcastState(tpStateDesc);
        // TODO 2.尝试匹配业务数据和配置表数据
        // 获取业务表数据
        final JSONObject bzData = value.getJSONObject(FlinkCDCConstants.DATA);
        // 获取业务(数据库)表名和对应操作
        final String sourceTableName = value.getString(FlinkCDCConstants.TABLE_NAME);
        // 获取操作类型
        final String opType = value.getString(FlinkCDCConstants.TYPE);
        // 拼接主键
        final String pk = sourceTableName + "_" + opType;
        // 通过主键从状态中获取对应的表配置(可能为空,因为可能不需要该表的这个行为)
        final TableProcess tp = tpState.get(pk);
        if (tp == null) {
            System.out.println("操作: " + pk + "不存在于配置表中");
        } else {
            // 匹配上配置了,将业务数据根据tp进行加工
            // (1) 过滤select的字段 (2) 从tp添加sink信息(sink表|主题)
            final JSONObject resObj = handleWithTableProcess(bzData, tp);

            // TODO 3.将维表数据写入侧输出流中
            if ("hbase".equals(tp.getSinkType())) {
                ctx.output(dimOt, resObj);
            } else {
                // TODO 4.将事实表从主流写出
                out.collect(resObj);
            }

        }
    }


    /**
     * 处理配置表的广播流数据
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        // TODO 1.解析配置表数据
        // 从cdc反序列化结果中解析出表配置
        final String tpItemStr = JSON.parseObject(value).getString("data");
        final TableProcess tpItem = JSON.parseObject(tpItemStr, TableProcess.class);
        System.out.println(tpItem.toString());

        // TODO 2.判断是否是hbase维度表，使用(if not exists)创建表
        if ("hbase".equals(tpItem.getSinkType())) {
            // 如果是维度表数据，尝试创建表
            // 尝试创建Phoenix表
            attemptCreatePhoenixTable(tpItem);
        } // kafka事实数据写入时会自动创建主题

        // TODO 3.更新状态 key:tableName_operate value TableProcess
        final BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
        // 拼接主键
        final String pk = tpItem.getSourceTable() + "_" + tpItem.getOperateType();
        // 更新配置表状态
        tpState.put(pk, tpItem);
    }

    /**
     * 创建phoenix维度表
     * @param tpItem
     */
    private void attemptCreatePhoenixTable(TableProcess tpItem) {

        // 从配置表中获取建表所需信息
        // 获取表名
        final String sinkTable = tpItem.getSinkTable();
        // 获取select的字段
        final String sinkColumns = tpItem.getSinkColumns();
        // 获取主键(维表一般使用id为主键)
        final String sinkPk = tpItem.getSinkPk();
        // 获取扩展字段
        final String sinkExtend = tpItem.getSinkExtend();

        // 切分字段
        final String[] selects = sinkColumns.split(",");
        // 切分主键(如果sinkPk字段为null,默认使用id做主键)
        final String[] pks = (sinkPk == null ? "id" : sinkPk).split(",");

        // 拼接建表语句
        final String ddl = concatDDL(sinkTable, selects, pks, sinkExtend);
        System.out.println(ddl);

        // 使用phoenix客户端提交ddl
        try {
            final PreparedStatement pst = conn.prepareStatement(ddl);
            pst.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix ddl for " + sinkTable + " execute failed");
        }
    }

    /**
     * 动态拼接phoenix建表语句
     * @param sinkTable
     * @param selects
     * @param pks
     * @param sinkExtend
     * @return
     */
    private String concatDDL(String sinkTable, String[] selects, String[] pks, String sinkExtend) {

        final StringBuilder ddl = new StringBuilder("create table if not exists ");
        ddl.append(PhoenixConfig.HBASE_SCHEMA).append(".").append(sinkTable)
           .append("(");

        final List<String> pkList = Arrays.asList(pks);

        // 遍历select的字段(需要判断主键,和最后的位置)
        for (int i = 0; i < selects.length; i++) {
            final String field = selects[i];
            // 判断是否是主键
            if (pkList.contains(field)) {
                ddl.append(field).append(" varchar primary key");
            } else {
                ddl.append(field).append(" varchar");
            }
            // 判断是否加,
            if (i < selects.length - 1) {
                ddl.append(",");
            }
        }

        ddl.append(") ").append(sinkExtend == null ? "" : sinkExtend);
        return ddl.toString();
    }

    /**
     * 将业务数据根据配置表规则进行加工
     * @param bzData
     * @param tp
     */
    private JSONObject handleWithTableProcess(JSONObject bzData, TableProcess tp) {

        // TODO 1.获取需要的数据
        // 获取select的字段(一次构建，多次访问)
        final String sinkColumns = tp.getSinkColumns();
        final List<String> selects = Arrays.asList(sinkColumns.split(","));
//        // 获取sinkType hbase|kafka
//        final String sinkType = tp.getSinkType();
        // 获取sinkTable
        final String sinkTable = tp.getSinkTable();

        // TODO 2.对业务数据进行加工
        final Set<Map.Entry<String, Object>> fields = bzData.entrySet();
        // 移除(使用迭代器的remove)
        final Iterator<Map.Entry<String, Object>> fieldsIter = fields.iterator();
        while (fieldsIter.hasNext()) {
            final Map.Entry<String, Object> field = fieldsIter.next();
            final String fieldName = field.getKey();
            if (!selects.contains(fieldName)) {
                // 如果不是选择的字段，则删除(使用迭代器的remove)
                fieldsIter.remove();
            }
        }

        // 添加sink信息(sink类型 + 表|主题)
        final JSONObject resObj = new JSONObject();
        resObj.put("data", bzData);
        resObj.put("sinkTable", sinkTable);
//        bzData.put("sinkType", sinkType);
        resObj.put("sinkColumns", sinkColumns);

        return resObj;
    }

    @Override
    public void close() throws Exception {
        // 关闭phoenix客户端连接
        if (conn != null) {
            conn.close();
        }
    }
}
