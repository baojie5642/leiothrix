package xin.bluesky.leiothrix.server.action;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.task.TaskStaticInfo;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.action.exception.NoResourceException;
import xin.bluesky.leiothrix.server.action.exception.NotAllowedLaunchException;
import xin.bluesky.leiothrix.server.action.exception.WorkerProcessorLaunchException;
import xin.bluesky.leiothrix.server.bean.node.NodeInfo;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.google.common.collect.FluentIterable.from;
import static xin.bluesky.leiothrix.server.action.WorkerProcessorInvoker.WORKER_PROCESSOR_MEMORY;

/**
 * @author 张轲
 */
public class WorkerProcessorLauncher {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProcessorLauncher.class);

    private WorkerProcessorInvoker workerProcessorInvoker;

    private WorkerManager workerManager;

    private String taskId;

    private List<NodeInfo> allWorkers;

    private LaunchLog launchLog;

    private TaskStaticInfo taskStaticInfo;

    private boolean noResource = true;

    public WorkerProcessorLauncher(String taskId) {
        this.workerProcessorInvoker = new WorkerProcessorInvoker();
        this.workerManager = new WorkerManager();
        this.launchLog = new LaunchLog();
        this.taskId = taskId;
    }

    public LaunchLog launch() throws WorkerProcessorLaunchException, NoResourceException, NotAllowedLaunchException {

        before();

        LaunchLog launchLog = doLaunch();

        after();

        return launchLog;
    }

    protected void before() {
        this.allWorkers = workerManager.getAllWorkerInfoWithInitialMemory();
        this.taskStaticInfo = TaskStorage.getTaskStaticInfo(taskId);

        allWorkers.forEach(worker -> {
            final int numBeforeLaunch = workerProcessorInvoker.calAvailableProcessNum(worker);
            launchLog.setAvailableProcessNumBeforeLuanch(worker.getIp(), numBeforeLaunch);
        });

        logger.info("当前为任务[taskId={}]可分配的资源如下(每个进程占用{}m内存):\r\n{}",
                taskId,
                WORKER_PROCESSOR_MEMORY,
                CollectionsUtils2.toString(
                        from(allWorkers)
                                .transform((info) -> {
                                    return info.getIp() + ":可启动" + launchLog.getAvailableProcessNumBeforeLuanch(info.getIp()) + "个进程";
                                })
                                .toList()
                        , "\r\n")
        );

    }

    protected LaunchLog doLaunch() {
        BlockingQueue<NodeInfo> availableQueue = new ArrayBlockingQueue(allWorkers.size(), false, allWorkers);

        while (resourceIsNotEnough(taskId) && availableQueue.size() != 0) {//已启动资源是否足够该任务执行
            NodeInfo worker = availableQueue.poll();
            String ip = worker.getIp();

            try {
                int availableWorkerProcessNumber = calAvailableProcessNum(worker);
                if (availableWorkerProcessNumber <= 0) {
                    continue;
                }
                this.noResource = false;

                String workerJarPath = copyJar2WorkerIfNecessary(taskId, launchLog, taskStaticInfo.getJarPath(), ip);

                workerProcessorInvoker.invoke(taskId, taskStaticInfo.getMainClass(), ip, workerJarPath);

                launchLog.incProcessorNum(ip);
                availableQueue.offer(worker);

                Thread.sleep(1 * 1000);//等待5秒,延缓下一个进程的启动
            } catch (Exception e) {
                logger.error("在{}上启动worker进程失败[taskId={}],异常信息为:{}", ip, taskId, ExceptionUtils.getStackTrace(e));
            }

        }

        return launchLog;
    }

    protected void after() throws NoResourceException, WorkerProcessorLaunchException {
        if (noResource) {
            throw new NoResourceException();
        }
        if (launchLog.getTotalProcessorNum() == 0) {
            throw new WorkerProcessorLaunchException("在所有worker上启动worker均失败");
        } else {
            logger.info("所有可用资源分配完毕,总共为任务[taskId={}]分配{}个worker进程,分配情况:{}", taskId, launchLog.getTotalProcessorNum(), printDistribute(allWorkers, launchLog));
        }

        TaskStorage.setStatus(taskId, TaskStatus.PROCESSING);
    }

    public boolean resourceIsNotEnough(String taskId) {
        return !TaskStorage.isResourceEnough(taskId);
    }

    protected int calAvailableProcessNum(NodeInfo worker) {
        // 根据zk的记录,来得到的可用进程数
        int accuracy = workerProcessorInvoker.calAvailableProcessNum(worker);

        // 根据启动记录,得到的可用进程数
        int numBeforeLaunch = launchLog.getAvailableProcessNumBeforeLuanch(worker.getIp());
        int estimate = numBeforeLaunch - launchLog.getProcessorNum(worker.getIp());

        // 取小值.正常情况下,这两个值应该是一样大(除非多个task并行调度),取较小值的目的是为了避免worker进程无法启动导致这里会不断启动新进程
        return Math.min(accuracy, estimate);
    }

    private String copyJar2WorkerIfNecessary(String taskId, LaunchLog launchLog, String serverJarPath, String ip) throws Exception {
        String workerJarPath = launchLog.getJarPath(ip);
        if (StringUtils.isBlank(workerJarPath)) {
            workerJarPath = workerManager.copyJar2Worker(taskId, serverJarPath, ip);
            launchLog.addJarPath(ip, workerJarPath);
        }
        return workerJarPath;
    }

    private String printDistribute(List<NodeInfo> allWorkers, LaunchLog launchLog) {
        StringBuffer buffer = new StringBuffer();
        allWorkers.forEach(node -> {
            String ip = node.getIp();
            int processNum = launchLog.getProcessorNum(ip);
            buffer.append("ip:" + ip + ",进程数:" + processNum + ";");
        });
        buffer.deleteCharAt(buffer.length() - 1);
        return buffer.toString();
    }

    public void setWorkerProcessorInvoker(WorkerProcessorInvoker workerProcessorInvoker) {
        this.workerProcessorInvoker = workerProcessorInvoker;
    }

    public void setWorkerManager(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }
}
