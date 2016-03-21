package xin.bluesky.leiothrix.server.action;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.action.exception.ProcessorLaunchException;
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

    protected static final int WORKER_MEMORY_REDUNDANCY = ServerConfigure.get("worker.memory.redundancy", Integer.class);

    public void invoke(String taskId, String mainClass, String workerIp, String workerJarPath) throws ProcessorLaunchException {

        try {
            String javaOpts = makeJavaOpts(workerIp);

            String propOpts = makePropOpts(taskId, workerIp);

            String command = makeCommand(javaOpts, propOpts, workerJarPath, mainClass);

            String remoteFullCommand = makeRemoteFullCommand(workerIp, command);
            logger.info("启动远程worker进程的命令:{}", remoteFullCommand);

            int exitValue = Runtime.getRuntime().exec(remoteFullCommand).waitFor();
            if (exitValue != 0) {
                throw new Exception(String.format("command=[%s],exitValue=[%s]", remoteFullCommand, exitValue));
            }
        } catch (Exception e) {
            throw new ProcessorLaunchException(String.format("在%s启动worker进程时失败", workerIp), e);
        }

    }

    private String makeJavaOpts(String workerIp) throws Exception {
        final String prop = System.getProperty("java.version");
        Float javaVersion = Float.parseFloat(prop.substring(0, prop.indexOf(".", prop.indexOf(".") + 1)));

        String javaOpts = StringUtils2.append(" -Xms", String.valueOf(WORKER_PROCESSOR_MEMORY), "m",
                " -server -Xmx", String.valueOf(WORKER_PROCESSOR_MEMORY), "m ",
                " -XX:NewRatio=", ServerConfigure.get("worker.processor.newratio"),
                " -XX:SurvivorRatio=", ServerConfigure.get("worker.processor.survivorratio"));

        // 根据java版本判断是否需要设置方法区,1.8以下的不需设置
        if (javaVersion.compareTo(Float.parseFloat("1.8")) < 0) {
            javaOpts = StringUtils2.append(javaOpts,
                    " -XX:MaxPermSize=", ServerConfigure.get("worker.processor.maxpermsize"), "m ");
        }

        // 设置gc log
        String userDir = getWorkerUserDir(workerIp);
        javaOpts = StringUtils2.append(javaOpts,
                " -XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps",
                " -Xloggc:", userDir, "/logs/gc.log -XX:GCLogFileSize=20M -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20",
                " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=", userDir, "/logs/dump");

        return javaOpts;
    }

    private String makePropOpts(String taskId, String workerIp) {
        return StringUtils2.append(" -Dserver.ip=", getServersIp(),
                " -Dserver.port=", ServerConfigure.get("server.port.worker"),
                " -Dworker.ip=", workerIp,
                " -DtaskId=", taskId,
                " -Dworker.range.pagesize=", ServerConfigure.get("worker.range.pagesize"),
                " -Dworker.processor.threadnum.factor=", ServerConfigure.get("worker.processor.threadnum.factor"));
    }

    private String makeCommand(String javaOpts, String propOpts, String workerJarPath, String mainClass) {
        return StringUtils2.append(ServerConfigure.get("worker.java"), "/bin/java ", javaOpts, propOpts, " -classpath ", workerJarPath, " ", mainClass, " >/dev/null &");
    }

    private String makeRemoteFullCommand(String workerIp, String command) {
        return CommandFactory.getRemoteFullCommand(command, ServerConfigure.get("worker.user"), workerIp);
    }

    private String getWorkerUserDir(String workerIp) throws Exception {
        String command = "`echo pwd`";
        String userDirCommand = makeRemoteFullCommand(workerIp, command);
        Process process = Runtime.getRuntime().exec(userDirCommand);
        return IOUtils.toString(process.getInputStream()).replace("\n", "");
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
        int upperProcessorNumber = freeMemory / (WORKER_PROCESSOR_MEMORY + WORKER_MEMORY_REDUNDANCY);

        return upperProcessorNumber;
    }

    /**
     * 以内存来计算可分配的worker进程数量.
     *
     * @param worker
     * @return
     */
    public int calAvailableProcessNum(NodeInfo worker) {
        // 已启动的进程数量
        int runningWorkerProcessorNumber = WorkerStorage.getWorkersNumber(worker.getIp());

        // 物理机可启动的进程上限
        int upperProcessorNumber = getPhysicalUpperLimitProcessNum(worker);

        // 配置所允许的最大进程数量
        String configMaxProcessorNumber = ServerConfigure.get("worker.processor.maxnum");
        int configured = 0;


        if (StringUtils.isNotBlank(configMaxProcessorNumber) && (configured = Integer.parseInt(configMaxProcessorNumber)) > 0) {
            return Math.min(configured, upperProcessorNumber) - runningWorkerProcessorNumber;
        } else {
            return upperProcessorNumber - runningWorkerProcessorNumber;
        }
    }
}
