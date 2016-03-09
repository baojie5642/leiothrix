package xin.bluesky.leiothrix.server.cache;

import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.bean.task.TaskRanges;

/**
 * @author 张轲
 */
public class TaskRangesCache {

    private static ExpiredMap<String, TaskRanges> map = new ExpiredMap<>(5 * 60);

    public static TaskRanges get(String taskId, String tableName) {
        String key = getKey(taskId, tableName);
        return map.get(key);
    }

    private static String getKey(String taskId, String tableName) {
        return StringUtils2.append(taskId, ":", tableName);
    }

    public static void put(TaskRanges taskRanges) {
        String key = getKey(taskRanges.getTaskId(), taskRanges.getTableName());
        map.put(key, taskRanges);
    }

    private TaskRangesCache() {

    }

}
