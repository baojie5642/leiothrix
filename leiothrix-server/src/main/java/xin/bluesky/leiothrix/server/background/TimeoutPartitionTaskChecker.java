package xin.bluesky.leiothrix.server.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.DateUtils2;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author 张轲
 * @date 16/3/3
 */
public class TimeoutPartitionTaskChecker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TimeoutPartitionTaskChecker.class);

    private static ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("timeout-range-checker").build());

    private static final long CHECK_INTERVAL = 30;

    protected static int RANGE_UPDATED_THRESHOLD = 2 * 60;

    public static void start() {
        logger.info("启动超时任务片检查线程,每隔{}秒检查一次", CHECK_INTERVAL);
        checker.scheduleAtFixedRate(new TimeoutPartitionTaskChecker(), 0, CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    protected TimeoutPartitionTaskChecker() {

    }

    @Override
    public void run() {
        List<String> taskList = TaskStorage.getAllProcessingTasks();

        if (CollectionsUtils2.isEmpty(taskList)) {
            return;
        }

        taskList.forEach(taskId -> {
            List<String> tableNameList = TableStorage.getAllTablesByTaskId(taskId);
            tableNameList.forEach(tableName -> {
                TableStatus tableStatus = TableStorage.getStatus(taskId, tableName);
                if (tableStatus == TableStatus.PROCESSING || tableStatus == TableStatus.WAIT_FOR_REALLOCATE) {
                    check(taskId, tableName);
                }
            });
        });
    }

    /**
     * 检查Range的状态.
     *
     * @param taskId
     * @param tableName
     * @return
     */
    private void check(String taskId, String tableName) {

        List<String> rangeNames = RangeStorage.getAllRangesByTableName(taskId, tableName);

        for (String rangeName : rangeNames) {
            long lastUpdateTime = RangeStorage.getRangeLastUpdateTime(taskId, tableName, rangeName);
            RangeStatus status = RangeStorage.getRangeStatus(taskId, tableName, rangeName);
            // 该节点是正在处理中的
            if (RangeStatus.PROCESSING == status || RangeStatus.PRE_ALLOCATE == status) {
                // 如果一段时间内未收到worker的进度更新,则判定worker已经die,需要重置状态以等待下一个worker来拿
                if (new Date().getTime() - lastUpdateTime > RANGE_UPDATED_THRESHOLD * 1000) {
                    RangeStorage.setRangeStatus(taskId, tableName, rangeName, RangeStatus.UNALLOCATED);
                    TableStorage.setStatus(taskId, tableName, TableStatus.WAIT_FOR_REALLOCATE);
                    logger.info("该节点[taskId={},tableName={},rangeName={}]上次更新时间为:{},距当前已经超过{}秒,判定其worker die,重置状态为未分配,并等待重新分配",
                            taskId, tableName, rangeName, DateUtils2.formatFull(lastUpdateTime), RANGE_UPDATED_THRESHOLD);
                }
            }

        }

    }
}
