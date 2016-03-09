package xin.bluesky.leiothrix.server.cache;

import xin.bluesky.leiothrix.server.bean.task.TaskTables;

/**
 * @author 张轲
 */
public class TaskTablesCache {

    private static ExpiredMap<String, TaskTables> map = new ExpiredMap<>(5 * 60);

    public static TaskTables get(String taskId) {
        return map.get(taskId);
    }

    public static void put(TaskTables taskTables) {
        map.put(taskTables.getTaskId(), taskTables);
    }

    private TaskTablesCache() {
    }
}
