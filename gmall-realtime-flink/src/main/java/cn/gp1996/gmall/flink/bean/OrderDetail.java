package cn.gp1996.gmall.flink.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * 订单明细数据
 */
@Data
public class OrderDetail {
    Long id;                            // 订单明细id
    Long order_id;                      // 订单id
    Long sku_id;                        // sku商品id
    BigDecimal order_price;             // 下单时价格
    Long sku_num;                       // 购买总数
    String sku_name;                    // 商品名称
    String create_time;                 // 创建时间
    BigDecimal split_total_amount;      // 拆单订单金额
    BigDecimal split_activity_amount;   // 拆单减免金额
    BigDecimal split_coupon_amount;     // 拆单优惠金额
    Long create_ts;                     // 创建时间戳
}
