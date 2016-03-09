package xin.bluesky.leiothrix.server.util;

import org.apache.commons.lang3.StringUtils;
import xin.bluesky.leiothrix.common.net.NetUtils;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;

import java.io.File;

/**
 * @author 张轲
 */
public class LeiothrixUtils {

    private static final String TASK_STORE_PATH = ServerConfigure.get("task.file.store");

    public static String getMyIp() {
        String serverIp = ServerConfigure.get("server", false);
        if (StringUtils.isBlank(serverIp)) {
            serverIp = NetUtils.getLocalIp();
        }

        return serverIp;
    }

    public static File getTaskDirPath(String taskId) {
        return new File(StringUtils2.append(TASK_STORE_PATH, File.separator, taskId));
    }
}
