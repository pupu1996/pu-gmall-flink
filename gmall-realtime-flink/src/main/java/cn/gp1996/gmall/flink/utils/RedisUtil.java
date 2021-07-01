package cn.gp1996.gmall.flink.utils;

import cn.gp1996.gmall.flink.constants.GmallConstants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author  gp1996
 * @date    2021-06-30
 * @desc    初始化jedis连接池
 */
public class RedisUtil {

    // Jedis连接池单例
    private static JedisPool jedisPool;


    // 在open方法中显示初始化jedis连接池
    public static void init() {
        if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    realInit();
                }
            }
        }
    }


    // 初始化Jedis连接池
    private static void realInit() {
        System.out.println("---------- 初始化jedis连接池 -----------");
        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setTestOnBorrow(true);// 取的时候ping测试一下，会有些许的性能开销
        jedisPool = new JedisPool(jedisPoolConfig, GmallConstants.REDIS_SERVER, GmallConstants.REDIS_PORT);
    }

    // 获取jedis连接
    public static Jedis getJedis() {
        if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    init();
                }
            }
        }

        return jedisPool.getResource();
    }
}
