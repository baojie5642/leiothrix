package xin.bluesky.leiothrix.worker.conf;

import com.google.common.base.Preconditions;

import static xin.bluesky.leiothrix.common.util.StringUtils2.COMMA;

/**
 * @author 张轲
 */
public class SettingInit {

    public static void init(WorkerConfiguration configuration) {
        Settings.setConfiguration(configuration);

        String serverIpConfig = System.getProperty("server.ip");
        Preconditions.checkNotNull(serverIpConfig, "需要配置server地址");
        Settings.setServersIp(serverIpConfig.split(COMMA));

        Settings.setServerPort(Integer.parseInt(System.getProperty("server.port")));

        Settings.setWorkerIp(System.getProperty("worker.ip"));

        Settings.setTaskId(System.getProperty("taskId"));

        Settings.setRangePageSize(Integer.parseInt(System.getProperty("worker.range.pagesize")));

        Settings.setThreadNumFactor(Integer.parseInt(System.getProperty("worker.processor.threadnum.factor")));
    }
}
