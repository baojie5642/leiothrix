package xin.bluesky.leiothrix.server.interactive.worker;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.conf.ConfigureException;

import java.io.IOException;
import java.util.Properties;

/**
 * @author 张轲
 */
public class CommandFactory {

    private static final Logger logger = LoggerFactory.getLogger(CommandFactory.class);

    private static final String REMOTE_COMMAND = "ssh -l %s %s %s";// ssh -l #user #ip #command

    private static Properties properties;

    static {
        properties = new Properties();
        try {
            properties.load(CommandFactory.class.getResourceAsStream("/command.properties"));
        } catch (IOException e) {
            throw new ConfigureException(String.format("加载配置文件时异常,异常信息为", ExceptionUtils.getStackTrace(e)));
        }
    }

    public static String get(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new ConfigureException(String.format("没有key=%s对应的配置项", key));
        }

        return value;
    }

    public static String getRemoteFullCommandByKey(String key, String user, String remoteIp) {
        String command = get(key);
        return String.format(REMOTE_COMMAND, user, remoteIp, command);
    }

    public static String getRemoteFullCommand(String command, String user, String remoteIp) {
        return String.format(REMOTE_COMMAND, user, remoteIp, command);
    }
}
