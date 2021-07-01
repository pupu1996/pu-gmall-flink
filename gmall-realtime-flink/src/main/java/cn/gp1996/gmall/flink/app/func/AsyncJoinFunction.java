package cn.gp1996.gmall.flink.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/***
 * 声明了异步维表join需要的接口
 * @param <T>
 */
public interface AsyncJoinFunction<T> {
    /**
     * 根据输入的数据获取要进行join的key的值
     * @param input
     * @return
     */
    String getKeyValue(T input);

    /**
     * 对查询的维表数据和业务数据进行join
     * @param input
     * @param dimInfo
     */
    void join(T input, JSONObject dimInfo) throws ParseException;
}
