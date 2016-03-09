package xin.bluesky.leiothrix.server.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.action.exception.WorkerProcessorLaunchException;
import xin.bluesky.leiothrix.server.bean.node.NodeInfo;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.interactive.worker.CommandFactory;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * @author 张轲
 */
public class WorkerManager {
    private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);

    public List<NodeInfo> getAllWorkerInfoWithInitialMemory() {
        List<NodeInfo> nodeInfoList = new ArrayList<>();

        List<String> workerIps = WorkerStorage.getAllWorkers();
        workerIps.forEach(ip -> {
            NodePhysicalInfo physicalInfo = WorkerStorage.getPhysicalInfo(ip);
            Integer workersNumber = WorkerStorage.getWorkersNumber(physicalInfo.getIp());
            nodeInfoList.add(new NodeInfo(physicalInfo, workersNumber));
        });

        return nodeInfoList;
    }

    public List<NodePhysicalInfo> refreshAndGetAllWorkerPhysicalInfo() {
        List<NodePhysicalInfo> nodePhysicalInfoList = new ArrayList();

        String workerUser = ServerConfigure.get("worker.user");
        //不是从配置文件中取,是因为worker是可以在运行时动态添加的
        List<String> workerIps = WorkerStorage.getAllWorkers();

        workerIps.forEach(ip -> {
            try {
                NodePhysicalInfo info = new NodePhysicalInfo(workerUser, ip);
                info.retrieveCpuInfo();
                info.retrieveMemoryInfo();

                WorkerStorage.savePhysicalInfo(info);

                nodePhysicalInfoList.add(info);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                WorkerStorage.delete(ip);
            }
        });

        nodePhysicalInfoList.sort((o1, o2) -> {
            return o1.getMemoryFree() > o2.getMemoryFree() ? -1 : 1;
        });

        return nodePhysicalInfoList;
    }

    public List<NodeInfo> refreshAndGetAllWorkerInfo() {
        List<NodePhysicalInfo> nodePhysicalInfoList = refreshAndGetAllWorkerPhysicalInfo();

        List<NodeInfo> nodeInfoList = new ArrayList();
        nodePhysicalInfoList.forEach(physicalInfo -> {
            Integer workersNumber = WorkerStorage.getWorkersNumber(physicalInfo.getIp());
            nodeInfoList.add(new NodeInfo(physicalInfo, workersNumber));
        });

        return nodeInfoList;
    }

    public String copyJar2Worker(String taskId, String absoluteJarName, String workerIp) throws Exception {
        String workerJarDir = getTaskDirOnWorker(taskId);

        // 在worker上创建目录
        String mkdirCommand = CommandFactory.getRemoteFullCommand(
                StringUtils2.append("mkdir -p ", workerJarDir), ServerConfigure.get("worker.user"), workerIp);
        if (Runtime.getRuntime().exec(mkdirCommand).waitFor() != 0) {
            throw new WorkerProcessorLaunchException(String.format("在worker[%s]上创建目录时失败,command:%s", workerIp, mkdirCommand));
        }

        // 拷贝jar到worker的相应目录
        String scpCommand = StringUtils2.append("scp ", absoluteJarName, " ", ServerConfigure.get("worker.user"), "@", workerIp, ":", workerJarDir, File.separator);
        int exitValue = Runtime.getRuntime().exec(scpCommand).waitFor();
        if (exitValue != 0) {
            throw new WorkerProcessorLaunchException(String.format("在拷贝jar文件到worker[%s]时失败,scpCommand:%s,exitValue=[%s]",
                    workerIp, scpCommand, exitValue));
        }

        return StringUtils2.append(workerJarDir, File.separator, new File(absoluteJarName).getName());
    }

    private String getTaskDirOnWorker(String taskId) {
        return StringUtils2.append(ServerConfigure.get("worker.file.store"), File.separator, taskId);
    }
}
