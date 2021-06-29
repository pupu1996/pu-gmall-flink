package cn.gp1996.gmall.flink.bean;

import lombok.Data;

/**
 * @author  gp1996
 * @date    2021-06-26
 * @desc    表配置Bean类
 */
@Data
public class TableProcess {

    // 源表46张业务表
    private String sourceTable;

    // 操作类型 insert update
    private String operateType;

    // sink类型 事实:kafka 维度:hbase
    private String sinkType;

    // sink表|主题
    private String sinkTable;

    // 过滤字段|创建phoenix表|sink到kafka的列
    private String sinkColumns;

    // 指定表的pk,在phoenix创建表需要指定pk —> 对应hbase的rowKey
    private String sinkPk;

    // 建表扩展字段
    private String sinkExtend;

    public enum SinkTypes {

        HBASE("hbase"),
        KAFKA("kafka");

        private final String type;

        static final String test111 = "DFSDFD";

        SinkTypes(String type) {this.type = type;}
    }
}
