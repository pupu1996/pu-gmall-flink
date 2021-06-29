package cn.gp1996.gmall.flink.bean;

import lombok.Data;

import java.math.BigDecimal; // java也有BigDecimal类型

/**
 * @desc    OrderInfo Bean
 */
@Data
public class OrderInfo {
    Long id;                            // 订单id
    Long province_id;                   // 省份id
    String order_status;                // 订单状态
    Long user_id;                       // 用户id
    BigDecimal total_amount;            // 订单最终总金额
    BigDecimal activity_reduce_amount;  // 活动减免金额
    BigDecimal coupon_reduce_amount;    // 优惠金额
    BigDecimal original_total_amount;   // 原始最终金额
    BigDecimal feight_fee;              // 运费
    String expire_time;                 // 过期时间
    String create_time;                 // 创建时间
    String operate_time;                // 操作时间
    String create_date;                 // 创建日期
    String create_hour;                 // 创建小时
    Long create_ts;                     // 创建时间戳(注意时区,flink系统默认使用0时区)
}