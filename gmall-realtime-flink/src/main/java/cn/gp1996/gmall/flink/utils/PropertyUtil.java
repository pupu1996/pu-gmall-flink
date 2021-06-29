package cn.gp1996.gmall.flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {

    public static Properties resolveProperties(String confPath) {

        // 1.新建Properties对象
        final Properties properties = new Properties();

        // 2.读入配置文件流
        final InputStream confStream =
                PropertyUtil.class.getClassLoader().getResourceAsStream(confPath);

        try {
            properties.load(confStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;
    }
}
