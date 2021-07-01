package cn.gp1996.gmall.flink.utils;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;

/**
 * @author  gp1996
 * @date    2021-06-29
 * @desc    时间日期工具类
 */
public class DateUtil {

    public static final String DATE_TIME_STR = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_STR = "yyyy-MM-dd";

    // simpleDateFormat有线程安全问题
    public static final SimpleDateFormat DATE_TIME_FORMAT =
            new SimpleDateFormat(DATE_TIME_STR);

    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern(DATE_STR);

    public static int birth2age(long ts) {
        final long curTs = System.currentTimeMillis();
        return Integer.parseInt(Long.toString(((curTs - ts) / (1000 * 60 * 60 * 24 * 365L))));
    }

    public static void main(String[] args) {
//        final String date = "2021-06-11";
//        System.out.println(birth2age(date));
    }


}
