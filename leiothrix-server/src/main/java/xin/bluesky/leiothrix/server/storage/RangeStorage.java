package xin.bluesky.leiothrix.server.storage;

import xin.bluesky.leiothrix.model.task.partition.ExecutionStatistics;
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

    public static final String NAME_RANGE_RECORD_NUM = "recordNum";

    public static final String NAME_STAT = "statistics";

    public static final String NAME_STAT_RECORD_NUM = "handledRecordNum";

    public static final String NAME_STAT_RECORD_NUM_SUCCESS = "successRecordNum";

    public static final String NAME_STAT_RECORD_NUM_FAIL = "failRecordNum";

    public static final String NAME_STAT_FAIL_PAGE = "failPage";

    public static final String NAME_STAT_EXCEPTION_STACK_TRACE = "exceptionStackTrace";

    public static final String NAME_STAT_QUERY_USING_TIME = "queryUsingTime";

    public static final String NAME_STAT_HANDLE_USING_TIME = "handleUsingTime";

    public static final String NAME_STAT_TOTAL_TIME = "totalTime";

    public static void createRange(String taskId, String tableName, String rangeName) {
        String[] r = rangeName.split(Constant.RANGE_SEPARATOR);
        final String rangePath = getRangePath(taskId, tableName, rangeName);
        createNodeAndSetData(rangePath, NAME_RANGE_START_INDEX, r[0]);
        createNodeAndSetData(rangePath, NAME_RANGE_END_INDEX, r[1]);
        createNodeAndSetData(rangePath, NAME_STATUS, RangeStatus.UNALLOCATED.name());
        createNodeAndSetData(rangePath, NAME_TIMEOUT_FLAG, String.valueOf(new Date().getTime()));
    }

    public static void createRange(String taskId, String tableName, String rangeName, int recordNum) {
        createRange(taskId, tableName, rangeName);

        final String rangePath = getRangePath(taskId, tableName, rangeName);
        createNodeAndSetData(rangePath, NAME_RANGE_RECORD_NUM, String.valueOf(recordNum));
    }

    public static PartitionTask getPartitionTask(String taskId, String tableName, String rangeName) {
        String rangePath = getRangePath(taskId, tableName, rangeName);

        int startIndex = getDataInteger(makePath(rangePath, NAME_RANGE_START_INDEX));
        int endIndex = getDataInteger(makePath(rangePath, NAME_RANGE_END_INDEX));
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
        String rangeStatus = getDataString(rangeStatusPath);
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

    public static void updatePageExecutionStatistics(String taskId, String tableName, String rangeName, ExecutionStatistics stat) {
        String path = getStatisticsPath(taskId, tableName, rangeName);

        addIntValue(makePath(path, NAME_STAT_RECORD_NUM), stat.getHandledRecordNum());

        addIntValue(makePath(path, NAME_STAT_RECORD_NUM_SUCCESS), stat.getSuccessRecordNum());
        if (stat.getFailRecordNum() != 0) {
            addIntValue(makePath(path, NAME_STAT_RECORD_NUM_FAIL), stat.getFailRecordNum());
            appendValue(makePath(path, NAME_STAT_FAIL_PAGE), stat.getFailPageName() + ";");
            appendValue(makePath(path, NAME_STAT_EXCEPTION_STACK_TRACE), stat.getExceptionMsg() + "\r\n");
        }

        addLongValue(makePath(path, NAME_STAT_QUERY_USING_TIME), stat.getQueryUsingTime());

        addLongValue(makePath(path, NAME_STAT_HANDLE_USING_TIME), stat.getHandleUsingTime());

        addLongValue(makePath(path, NAME_STAT_TOTAL_TIME), stat.getTotalTime());
    }

    public static ExecutionStatistics getExecutionStatistics(String taskId, String tableName, String rangeName) {
        String path = getStatisticsPath(taskId, tableName, rangeName);
        if (!checkExists(path)) {
            return null;
        }

        ExecutionStatistics stat = new ExecutionStatistics();

        stat.setHandledRecordNum(getDataInteger(makePath(path, NAME_STAT_RECORD_NUM)));

        stat.setSuccessRecordNum(getDataInteger(makePath(path, NAME_STAT_RECORD_NUM_SUCCESS)));

        if (checkExists(makePath(path, NAME_STAT_RECORD_NUM_FAIL))) {
            stat.setFailRecordNum(getDataInteger(makePath(path, NAME_STAT_RECORD_NUM_FAIL)));
            stat.setFailPageName(getDataString(makePath(path, NAME_STAT_FAIL_PAGE)));
            stat.setExceptionMsg(getDataString(makePath(path, NAME_STAT_EXCEPTION_STACK_TRACE)));
        }

        stat.setQueryUsingTime(getDataLong(makePath(path, NAME_STAT_QUERY_USING_TIME)));

        stat.setHandleUsingTime(getDataLong(makePath(path, NAME_STAT_HANDLE_USING_TIME)));

        stat.setTotalTime(getDataLong(makePath(path, NAME_STAT_TOTAL_TIME)));

        return stat;
    }

    private static String getStatisticsPath(String taskId, String tableName, String rangeName) {
        return makePath(getRangePath(taskId, tableName, rangeName), NAME_STAT);
    }
}
