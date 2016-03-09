package xin.bluesky.leiothrix.server.cache;

import xin.bluesky.leiothrix.model.task.TaskStaticInfo;

/**
 * @author 张轲
 */
public class TaskStaticInfoCache {

    private static ExpiredMap<String, TaskStaticInfo> map = new ExpiredMap<>(5 * 60);

    public static TaskStaticInfo get(String taskId) {
        return map.get(taskId);
    }

    public static void put(String taskId, TaskStaticInfo taskStaticInfo) {
        map.put(taskId, taskStaticInfo);
    }

    private TaskStaticInfoCache() {

    }


}
