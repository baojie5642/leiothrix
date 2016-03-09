package xin.bluesky.leiothrix.server.cache;

import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

/**
 * @author 张轲
 */
public class FinishedComponentCache {

    private static ExpiredMap<String, Object> partitionTaskMap = new ExpiredMap<>(5 * 60);

    private static ExpiredMap<String, Object> tableMap = new ExpiredMap<>(10 * 60);

    private static ExpiredMap<String, Object> taskMap = new ExpiredMap<>(10 * 60);

    private static FinishedComponentCache cache = new FinishedComponentCache();

    private FinishedComponentCache() {

    }

    public static FinishedComponentCache getInstance() {
        return cache;
    }

    public boolean isPartitionTaskFinished(String taskId, String tableName, String rangeName) {
        String key = StringUtils2.append(taskId, ":", tableName, ":", rangeName);
        if (partitionTaskMap.containsKey(key)) {
            return true;
        }

        RangeStatus status = RangeStorage.getRangeStatus(taskId, tableName, rangeName);
        if (status == RangeStatus.FINISHED) {
            partitionTaskMap.put(key, new Object());
            return true;
        }

        return false;
    }

    public boolean isTableFinished(String taskId, String tableName) {
        String key = StringUtils2.append(taskId, ":", tableName);
        if (tableMap.containsKey(key)) {
            return true;
        }

        TableStatus status = TableStorage.getStatus(taskId, tableName);
        if (status == TableStatus.FINISHED) {
            tableMap.put(key, new Object());
            return true;
        }

        return false;
    }

    public boolean isTaskFinished(String taskId) {
        if (taskMap.containsKey(taskId)) {
            return true;
        }

        TaskStatus status = TaskStorage.getStatus(taskId);
        if (status == TaskStatus.FINISHED) {
            taskMap.put(taskId, new Object());
            return true;
        }

        return false;
    }

}
