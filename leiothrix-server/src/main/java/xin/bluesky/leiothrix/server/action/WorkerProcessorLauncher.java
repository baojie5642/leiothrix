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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.google.common.collect.FluentIterable.from;
import static xin.bluesky.leiothrix.server.action.WorkerProcessorInvoker.WORKER_PROCESSOR_MEMORY;

/**
 * @author 张轲
 */
public class WorkerProcessorLauncher {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProcessorLauncher.class);

    private WorkerProcessorInvoker workerProcessorInvoker = new WorkerProcessorInvoker();

    private WorkerManager workerManager = new WorkerManager();

    public void launch(String taskId) throws WorkerProcessorLaunchException, NoResourceException, NotAllowedLaunchException {
        List<NodeInfo> allWorkers = workerManager.getAllWorkerInfoWithInitialMemory();

        logger.info("开始计算worker[{}]上的资源,如资源足够,则启动worker进程.每个worker进程需要占用{}m内存",
                CollectionsUtils2.toString(from(allWorkers).transform((info) -> info.getPhysicalInfo().getIp()).toList()), WORKER_PROCESSOR_MEMORY);

        int successProcessorNum = 0;
        boolean noResource = true;
        LaunchLog launchLog = new LaunchLog();

        TaskStaticInfo taskStaticInfo = TaskStorage.getTaskStaticInfo(taskId);

        BlockingQueue<NodeInfo> availableQueue = new ArrayBlockingQueue(allWorkers.size(), false, allWorkers);
        while (resourceIsNotEnough(taskId) && availableQueue.size() != 0) {//已启动资源是否足够该任务执行
            NodeInfo worker = availableQueue.poll();
            String ip = worker.getPhysicalInfo().getIp();

            try {
                int availableWorkerProcessNumber = workerProcessorInvoker.calMaxWorkerProcess(worker);
                if (availableWorkerProcessNumber <= 0) {
                    logger.info("在{}上没有可用资源来启动worker进程[taskId={}]", ip, taskId);
                    continue;
                }
                logger.info("在{}上的当前资源可以为任务[taskId={}]启动{}个进程", ip, taskId, availableWorkerProcessNumber);
                noResource = false;

                String workerJarPath = copyJar2WorkerIfNecessary(taskId, launchLog, taskStaticInfo.getJarPath(), ip);

                workerProcessorInvoker.invoke(taskId, taskStaticInfo.getMainClass(), ip, workerJarPath);

                successProcessorNum++;
                launchLog.incProcessorNum(ip);
                availableQueue.offer(worker);

                Thread.sleep(5 * 1000);//等待5秒,延缓下一个进程的启动
            } catch (Exception e) {
                logger.error("在{}上启动worker进程失败[taskId={}],异常信息为:{}", ip, taskId, ExceptionUtils.getStackTrace(e));
            }

        }

        if (noResource) {
            throw new NoResourceException();
        }
        if (successProcessorNum == 0) {
            throw new WorkerProcessorLaunchException("在所有worker上启动worker均失败");
        } else {
            logger.info("所有可用资源分配完毕,总共为任务[taskId={}]分配{}个worker进程,分配情况:{}", taskId, successProcessorNum, printDistribute(allWorkers, launchLog));
        }

        TaskStorage.setStatus(taskId, TaskStatus.PROCESSING);
    }

    public boolean resourceIsNotEnough(String taskId) {
        return !TaskStorage.isResourceEnough(taskId);
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
            String ip = node.getPhysicalInfo().getIp();
            int processNum = launchLog.getProcessorNum(ip);
            buffer.append("ip:" + ip + ",进程数:" + processNum + ";");
        });
        buffer.deleteCharAt(buffer.length() - 1);
        return buffer.toString();
    }

    private class LaunchLog {

        private Map<String, LogDetail> map = new HashMap();

        private LogDetail createLogDetailIfAbsent(String workerIp) {
            LogDetail detail = map.get(workerIp);
            if (detail == null) {
                detail = new LogDetail();
                map.put(workerIp, detail);
            }
            return detail;
        }

        public void addJarPath(String workerIp, String jarPath) {
            LogDetail detail = createLogDetailIfAbsent(workerIp);
            detail.setJarPath(jarPath);
        }

        public void incProcessorNum(String workerIp) {
            LogDetail detail = createLogDetailIfAbsent(workerIp);
            detail.setProcessorNum(detail.getProcessorNum() + 1);
        }

        public String getJarPath(String workerIp) {
            LogDetail detail = createLogDetailIfAbsent(workerIp);
            return detail.getJarPath();
        }

        public int getProcessorNum(String workerIp) {
            LogDetail detail = createLogDetailIfAbsent(workerIp);
            return detail.getProcessorNum();
        }

        private class LogDetail {

            private String jarPath;

            private int processorNum;

            public String getJarPath() {
                return jarPath;
            }

            public void setJarPath(String jarPath) {
                this.jarPath = jarPath;
            }

            public int getProcessorNum() {
                return processorNum;
            }

            public void setProcessorNum(int processorNum) {
                this.processorNum = processorNum;
            }
        }
    }
}
