package xin.bluesky.leiothrix.server.storage;

import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.task.TaskRanges;
import xin.bluesky.leiothrix.server.cache.TaskRangesCache;
import xin.bluesky.leiothrix.server.lock.LockFactory;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static xin.bluesky.leiothrix.server.storage.TableStorage.NAME_TABLES;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.*;

/**
 * @author 张轲
 */
public class RangeStorage {
    public static final String NAME_RANGE_START_INDEX = "startIndex";

    public static final String NAME_RANGE_END_INDEX = "endIndex";

    public static final String NAME_STATUS = "status";

    public static final String NAME_RANGES = "ranges";

    /**
     * 判断range是否处理超时的标志字段
     */
    public static final String NAME_TIMEOUT_FLAG = "timeoutFlag";

    public static void createRange(String taskId, String tableName, String rangeName) {
        String[] r = rangeName.split(Constant.RANGE_SEPARATOR);
        final String rangePath = getRangePath(taskId, tableName, rangeName);
        createNodeAndSetData(rangePath, NAME_RANGE_START_INDEX, r[0]);
        createNodeAndSetData(rangePath, NAME_RANGE_END_INDEX, r[1]);
        createNodeAndSetData(rangePath, NAME_STATUS, RangeStatus.UNALLOCATED.name());
        createNodeAndSetData(rangePath, NAME_TIMEOUT_FLAG, String.valueOf(new Date().getTime()));
    }

    public static PartitionTask getPartitionTask(String taskId, String tableName, String rangeName) {
        String rangePath = getRangePath(taskId, tableName, rangeName);

        int startIndex = Integer.parseInt(getDataByString(makePath(rangePath, NAME_RANGE_START_INDEX)));
        int endIndex = Integer.parseInt(getDataByString(makePath(rangePath, NAME_RANGE_END_INDEX)));
        String primaryKey = TableStorage.getTableMeta(taskId, tableName).getPrimaryKey();

        PartitionTask partitionTask = new PartitionTask();
        partitionTask.setTaskId(taskId);
        partitionTask.setTableName(tableName);
        partitionTask.setPrimaryKey(primaryKey);
        partitionTask.setPartitionRangeName(rangeName);
        partitionTask.setRowStartIndex(startIndex);
        partitionTask.setRowEndIndex(endIndex);

        return partitionTask;
    }

    public static void setRangeStatus(String taskId, String tableName, String rangeName, RangeStatus status) {
        String rangeStatusPath = getRangeStatusPath(taskId, tableName, rangeName);
        ZookeeperUtils.setData(rangeStatusPath, status.name());
    }

    public static List<String> getAllRangesByTableName(String taskId, String tableName) {
        TaskRanges cached = TaskRangesCache.get(taskId, tableName);
        if (cached != null) {
            return cached.getRangeNameList();
        }

        ReentrantReadWriteLock lock = LockFactory.getTaskRangesCacheLock(taskId, tableName);
        lock.writeLock().lock();
        try {
            cached = TaskRangesCache.get(taskId, tableName);
            if (cached != null) {
                return cached.getRangeNameList();
            }
            String rangesPath = makePath(TaskStorage.TASKS, taskId, NAME_TABLES, tableName, NAME_RANGES);
            List<String> rangeNameList = getChildrenWithSimplePath(rangesPath);
            TaskRangesCache.put(new TaskRanges(taskId, tableName, rangeNameList));
            return rangeNameList;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static RangeStatus getRangeStatus(String taskId, String tableName, String rangeName) {
        String rangeStatusPath = getRangeStatusPath(taskId, tableName, rangeName);
        if (!checkExists(rangeStatusPath)) {
            return RangeStatus.NOT_EXIST;
        }
        String rangeStatus = getDataByString(rangeStatusPath);
        return RangeStatus.valueOf(rangeStatus);
    }

    public static long getRangeLastUpdateTime(String taskId, String tableName, String rangeName) {
        String timeoutFlagPath = makePath(getRangePath(taskId, tableName, rangeName), NAME_TIMEOUT_FLAG);
        return ZookeeperUtils.getNodeStat(timeoutFlagPath).getMtime();
    }

    public static void refreshRangeLastUpdateTime(String taskId, String tableName, String rangeName) {
        String timeoutFlagPath = makePath(getRangePath(taskId, tableName, rangeName), NAME_TIMEOUT_FLAG);
        ZookeeperUtils.setData(timeoutFlagPath, String.valueOf(new Date().getTime()));
    }

    public static void setNameRangeStartIndex(String taskId, String tableName, String rangeName, long startIndex) {
        setData(makePath(getRangePath(taskId, tableName, rangeName), NAME_RANGE_START_INDEX), String.valueOf(startIndex));
    }

    private static String getRangePath(String taskId, String tableName, String rangeName) {
        return makePath(TaskStorage.TASKS, taskId, NAME_TABLES, tableName, NAME_RANGES, rangeName);
    }

    private static String getRangeStatusPath(String taskId, String tableName, String rangeName) {
        return makePath(getRangePath(taskId, tableName, rangeName), NAME_STATUS);
    }
}
