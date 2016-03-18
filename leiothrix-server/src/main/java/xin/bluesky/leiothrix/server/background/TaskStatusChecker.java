package xin.bluesky.leiothrix.server.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.cache.PartitionTaskContainer;
import xin.bluesky.leiothrix.server.interactive.client.TaskFileService;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static xin.bluesky.leiothrix.server.bean.status.TableStatus.FINISHED;

/**
 * @author 张轲
 */
public class TaskStatusChecker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TaskStatusChecker.class);

    private static ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("task-status-checker").build());

    private static final long CHECK_INTERVAL = 30;

    public static void start() {
        logger.info("启动任务检查线程,每隔{}秒检查一次", CHECK_INTERVAL);
        checker.scheduleAtFixedRate(new TaskStatusChecker(), 0, CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    protected TaskStatusChecker() {

    }

    @Override
    public void run() {
        List<String> taskList = TaskStorage.getAllProcessingTasks();

        if (CollectionsUtils2.isEmpty(taskList)) {
            return;
        }

        taskList.forEach(taskId -> {
            List<String> tableNameList = TableStorage.getAllTablesByTaskId(taskId);
            if (isAllTableFinished(taskId, tableNameList)) {
                finishTask(taskId);
                logger.info("任务{}被执行完毕,更新其状态为已结束", taskId);
            }
        });
    }

    /**
     * 检查table的状态.如果有未完成的table,则返回true,否则返回false
     *
     * @param taskId
     * @param tableNameList
     * @return
     */
    private boolean isAllTableFinished(String taskId, List<String> tableNameList) {
        for (String tableName : tableNameList) {
            if (isTableFinished(taskId, tableName)) {
                TableStorage.setStatus(taskId, tableName, FINISHED);
            } else {
                return false;
            }
        }

        return true;
    }

    private boolean isTableFinished(String taskId, String tableName) {
        TableStatus tableStatus = TableStorage.getStatus(taskId, tableName);

        switch (tableStatus) {
            case FINISHED:
                return true;
            case UNALLOCATED:
            case WAIT_FOR_REALLOCATE:
                return false;
            case PROCESSING:
                List<String> rangeNames = RangeStorage.getAllRangesByTableName(taskId, tableName);
                for (String rangeName : rangeNames) {
                    final RangeStatus rangeStatus = RangeStorage.getRangeStatus(taskId, tableName, rangeName);
                    if (rangeStatus != RangeStatus.FINISHED) {
                        return false;
                    }
                }
                return true;
            default:
                return false;
        }
    }

    private void finishTask(String taskId) {
        TaskStorage.setStatus(taskId, TaskStatus.FINISHED);

        TaskStorage.logTaskFinishedTime(taskId);

        PartitionTaskContainer.evict(taskId);

        TaskFileService.deleteOnAllServers(taskId);

    }

}
