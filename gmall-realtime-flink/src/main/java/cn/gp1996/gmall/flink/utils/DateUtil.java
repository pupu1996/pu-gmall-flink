package cn.gp1996.gmall.flink.utils;

import java.text.SimpleDateFormat;

/**
 * @author  gp1996
 * @date    2021-06-29
 * @desc    时间日期工具类
 */
public class DateUtil {

    public static final String DATE_TIME_STR = "yyyy-MM-dd HH:mm:ss";

    public static final SimpleDateFormat DATE_TIME_FORMAT =
            new SimpleDateFormat(DATE_TIME_STR);
}
