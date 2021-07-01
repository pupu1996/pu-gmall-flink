package cn.gp1996.gmall.flink.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author  gp1996
 * @date    2021-06-30
 * @desc    返回自定义线程池单例
 */
public class ThreadPoolUtil {

    // 保存线程池单例
    private static ThreadPoolExecutor cachedThreadPool = null;

    // 创建一个可缓存线程池单例(适合执行大量的小任务)
    public  static ThreadPoolExecutor getCachedInstance() {
        if (cachedThreadPool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (cachedThreadPool == null) {
//                    cachedThreadPool = new ThreadPoolExecutor(
//                            0,
//                            Integer.MAX_VALUE,
//                            360,
//                            TimeUnit.SECONDS,
//                            new SynchronousQueue<Runnable>()
//                    );
                    cachedThreadPool = new ThreadPoolExecutor(
                            8,
                            8,
                            360,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }

        return cachedThreadPool;
    }
}
