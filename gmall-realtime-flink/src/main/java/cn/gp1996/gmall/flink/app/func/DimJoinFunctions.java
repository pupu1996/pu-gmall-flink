package cn.gp1996.gmall.flink.app.func;

import cn.gp1996.gmall.flink.bean.OrderDetail;
import cn.gp1996.gmall.flink.bean.OrderInfo;
import cn.gp1996.gmall.flink.bean.OrderWide;
import cn.gp1996.gmall.flink.utils.DateUtil;
import com.alibaba.fastjson.JSONObject;
import com.ctc.wstx.util.DataUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author  gp1996
 * @date    2021-06-30
 * @desc    维表异步join的具体实现(16张维度表)
 */
public class DimJoinFunctions {

    /**
     * join 用户维度表 dim_user_info
     */
    public static class JoinDimUserInfo
            extends GenericAsyncJoinFunction<OrderWide> {

        // 创建sdf;
        private static ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<>();

        public JoinDimUserInfo(String tableName) {
            super(tableName);
        }

        @Override
        public String getKeyValue(OrderWide input) {
            return input.getUser_id().toString();
        }

        @Override
        public void join(OrderWide input, JSONObject dimInfo) throws ParseException {

            sdf.set(new SimpleDateFormat("yyyy-MM-dd"));
            // TODO 1.从维表数据中获取需要的字段
            final String gender = dimInfo.getString("GENDER");
            final String timeStr = dimInfo.getString("BIRTHDAY");
            // 生日转年龄
            final long time = sdf.get().parse(timeStr).getTime();
            final int age = DateUtil.birth2age(time);

            // TODO 2.在数据对象设置维度值
            input.setUser_gender(gender);
            input.setUser_age(23);

        }

    }

    /**
     * join省份表
     */
    public static class JoinDimBaseProvince
            extends GenericAsyncJoinFunction<OrderWide> {

        public JoinDimBaseProvince(String tableName) {
            super(tableName);
        }

        @Override
        public String getKeyValue(OrderWide input) {
            return input.getProvince_id().toString();
        }

        @Override
        public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
            // join省份表的数据
            // 获取维度信息
            final String provinceName = dimInfo.getString("NAME");
            final String areaCode = dimInfo.getString("AREA_CODE");
            final String isoCode = dimInfo.getString("ISO_CODE");
            final String iso_3166_2 = dimInfo.getString("ISO_3166_2");
            // 插入到双流join的数据中
            input.setProvince_name(provinceName);
            input.setProvince_area_code(areaCode);
            input.setProvince_iso_code(isoCode);
            input.setProvince_3166_2_code(iso_3166_2);
        }
    }

    /**
     * join商品表
     */
    public static class JoinDimSkuInfo extends GenericAsyncJoinFunction<OrderWide> {

        public JoinDimSkuInfo(String tableName) {
            super(tableName);
        }

        @Override
        public String getKeyValue(OrderWide input) {
            return input.getSku_id().toString();
        }

        @Override
        public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
            // 获取维表信息中所需字段
            // 品牌id
            final Long tm_id = dimInfo.getLong("TM_ID");
            // 分类id
            final Long category3_id = dimInfo.getLong("CATEGORY3_ID");
            // spu_id
            final Long spu_id = dimInfo.getLong("SPU_ID");

            // 拼接数据
            input.setTm_id(tm_id);
            input.setCategory3_id(category3_id);
            input.setSpu_id(spu_id);
        }
    }

    /**
     * 根据品牌id join品牌表
     */
    public static class JoinDimBaseTradeMark
            extends GenericAsyncJoinFunction<OrderWide> {

        public JoinDimBaseTradeMark(String tableName) {
            super(tableName);
        }

        @Override
        public String getKeyValue(OrderWide input) {
            return input.getTm_id().toString();
        }

        @Override
        public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
            // 根据维度信息,获取所需字段
            final String tm_name = dimInfo.getString("TM_NAME");
            // 添加进字段
            input.setTm_name(tm_name);
        }
    }

    /**
     * 根据category3_id,join品类表
     */
    public static class JoinDimBaseCategory3
            extends GenericAsyncJoinFunction<OrderWide> {

        public JoinDimBaseCategory3(String tableName) {
            super(tableName);
        }

        @Override
        public String getKeyValue(OrderWide input) {
            return input.getCategory3_id().toString();
        }

        @Override
        public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
            // 从维度信息中提取所需字段
            final String category3_name = dimInfo.getString("NAME");
            input.setCategory3_name(category3_name);
        }
    }

    /**
     * 根据spu_Id join spu信息表
     */
    public static class JoinDimSpuInfo extends GenericAsyncJoinFunction<OrderWide> {

        public JoinDimSpuInfo(String tableName) {
            super(tableName);
        }

        @Override
        public String getKeyValue(OrderWide input) {
            return input.getSpu_id().toString();
        }

        @Override
        public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
            // 从维度表中获取相关信息
            final String spu_name = dimInfo.getString("SPU_NAME");
            input.setSpu_name(spu_name);
        }
    }

}
