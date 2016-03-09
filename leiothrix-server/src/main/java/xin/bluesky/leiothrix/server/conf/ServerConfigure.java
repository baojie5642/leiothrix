package xin.bluesky.leiothrix.server.conf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static xin.bluesky.leiothrix.server.Constant.SYS_PROP_CONFIG_FILE;

/**
 * @author 张轲
 */
public class ServerConfigure {

    private static final Logger logger = LoggerFactory.getLogger(ServerConfigure.class);

    private static Properties properties;

    static {
        properties = new Properties();
        try {
            final String systemConfig = System.getProperty(SYS_PROP_CONFIG_FILE);
            if (StringUtils.isNotBlank(systemConfig)) {
                properties.load(new FileInputStream(systemConfig));
            } else {
                properties.load(ServerConfigure.class.getResourceAsStream("/config.properties"));
            }
        } catch (IOException e) {
            logger.error("加载配置文件时异常,异常信息为:{}", ExceptionUtils.getStackTrace(e));
            throw new ConfigureException(e);
        }
    }

    public static void setProperties(Properties props) {
        properties = props;
    }

    public static void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public static String get(String key) {
        return get(key, true);
    }

    public static String get(String key, boolean mustExist) {
        String value = System.getProperty(key);
        if (value == null) {
            value = properties.getProperty(key);
        }

        if (value == null && mustExist) {
            throw new ConfigureException(String.format("没有key=%s对应的配置项", key));
        }

        return value;
    }

    public static <T> T get(String key, Class<T> claz) {
        String value = get(key);
        if (claz == Integer.class) {
            return (T) new Integer(Integer.parseInt(value));
        } else {
            throw new UnsupportedOperationException(String.format("不支持%s类型", claz.getSimpleName()));
        }
    }
}
