package xin.bluesky.leiothrix.worker;

/**
 * @author 张轲
 * @date 16/2/15
 */
public class Settings {

    private static WorkerConfiguration configuration;

    private static String[] serversIp;

    private static Integer serverPort;

    private static String taskId;

    private static int threadNumFactor;

    public static WorkerConfiguration getConfiguration() {
        return configuration;
    }

    public static void setConfiguration(WorkerConfiguration configuration) {
        Settings.configuration = configuration;
    }

    public static String[] getServersIp() {
        return serversIp;
    }

    public static void setServersIp(String[] serversIp) {
        Settings.serversIp = serversIp;
    }

    public static Integer getServerPort() {
        return serverPort;
    }

    public static void setServerPort(Integer serverPort) {
        Settings.serverPort = serverPort;
    }

    public static String getTaskId() {
        return taskId;
    }

    public static void setTaskId(String taskId) {
        Settings.taskId = taskId;
    }

    public static int getThreadNumFactor() {
        return threadNumFactor;
    }

    public static void setThreadNumFactor(int threadNumFactor) {
        Settings.threadNumFactor = threadNumFactor;
    }
}
