package xin.bluesky.leiothrix.server.action;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.action.exception.WorkerProcessorLaunchException;
import xin.bluesky.leiothrix.server.bean.node.NodeInfo;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.interactive.worker.CommandFactory;
import xin.bluesky.leiothrix.server.storage.ServerStorage;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;

import java.util.List;

import static xin.bluesky.leiothrix.common.util.StringUtils2.COMMA;

/**
 * @author 张轲
 */
public class WorkerProcessorInvoker {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProcessorInvoker.class);

    //给worker进程分配的内存,配置中的单位为M
    public static final int WORKER_PROCESSOR_MEMORY = ServerConfigure.get("worker.processor.memory", Integer.class);

    private static final int WORKER_MEMORY_REDUNDANCY = ServerConfigure.get("worker.memory.redundancy", Integer.class);

    public void invoke(String taskId, String mainClass, String workerIp, String workerJarPath) throws WorkerProcessorLaunchException {
        String javaOpts = StringUtils2.append(" -Xms", String.valueOf(WORKER_PROCESSOR_MEMORY), "m",
                " -Xmx", String.valueOf(WORKER_PROCESSOR_MEMORY), "m ");

        String serversIp = getServersIp();
        String propOpts = StringUtils2.append(" -Dserver.ip=", serversIp,
                " -Dserver.port=", ServerConfigure.get("server.port.worker"),
                " -Dworker.ip=", workerIp,
                " -DtaskId=", taskId,
                " -Dworker.processor.threadnum.factor=", ServerConfigure.get("worker.processor.threadnum.factor"));

        String command = StringUtils2.append(ServerConfigure.get("worker.java"), "/bin/java ", javaOpts, propOpts, " -classpath ", workerJarPath, " ", mainClass, " >/dev/null &");

        int exitValue;
        String remoteFullCommand;
        try {
            remoteFullCommand = CommandFactory.getRemoteFullCommand(
                    String.format(command, javaOpts),
                    ServerConfigure.get("worker.user"), workerIp);

            logger.info("启动远程worker进程的命令:{}", remoteFullCommand);
            final Process exec = Runtime.getRuntime().exec(remoteFullCommand);
            exitValue = exec.waitFor();
        } catch (Exception e) {
            throw new WorkerProcessorLaunchException(String.format("在%s启动worker进程时失败", workerIp));
        }

        if (exitValue != 0) {
            throw new WorkerProcessorLaunchException(String.format("在%s启动worker进程时失败.command=[%s],exitValue=[%s]",
                    workerIp, remoteFullCommand, exitValue));
        }
    }

    private String getServersIp() {
        List<String> allServers = ServerStorage.getAllServers();

        if (CollectionsUtils2.isEmpty(allServers)) {
            throw new RuntimeException("server列表为空,这是不应该发生的");
        }

        return CollectionsUtils2.toString(allServers, COMMA);
    }

    /**
     * 获得worker物理机的自身内存,所允许启动的进程数量上限
     *
     * @param worker
     * @return
     */
    public int getPhysicalUpperLimitProcessNum(NodeInfo worker) {
        NodePhysicalInfo physicalInfo = worker.getPhysicalInfo();
        int freeMemory = (int) (physicalInfo.getMemoryFreeBeforeAsWorker() >> 10);

        // 该worker总共可启动的进程数量
        int upperProcessorNumber = freeMemory / WORKER_PROCESSOR_MEMORY + WORKER_MEMORY_REDUNDANCY;

        return upperProcessorNumber;
    }

    /**
     * 以内存来计算可分配的worker进程数量.
     *
     * @param worker
     * @return
     */
    public int calAvailableProcessNum(NodeInfo worker) {
        // 物理机可启动的进程上限
        int upperProcessorNumber = getPhysicalUpperLimitProcessNum(worker);

        // 减去已启动的进程数量
        int runningWorkerProcessorNumber = WorkerStorage.getWorkersNumber(worker.getIp());
        int available = upperProcessorNumber - runningWorkerProcessorNumber;

        // 获取配置所允许的最大进程数量
        String configMaxProcessorNumber = ServerConfigure.get("worker.processor.maxnum");
        if (StringUtils.isBlank(configMaxProcessorNumber) || Integer.parseInt(configMaxProcessorNumber) <= 0) {
            return available;
        }

        return Math.min(available, Integer.parseInt(configMaxProcessorNumber));
    }
}
