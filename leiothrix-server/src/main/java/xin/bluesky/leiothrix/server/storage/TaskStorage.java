package xin.bluesky.leiothrix.server.storage;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.FluentIterable;
import org.apache.zookeeper.CreateMode;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.task.TaskConfig;
import xin.bluesky.leiothrix.model.task.TaskStaticInfo;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.cache.TaskStaticInfoCache;
import xin.bluesky.leiothrix.server.lock.LockFactory;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.*;

/**
 * @author 张轲
 * @date 16/2/17
 */
public class TaskStorage {
    public static final String TASKS = Constant.ROOT_DIR + "/tasks";

    public static final String NAME_JAR = "jar";

    public static final String NAME_CONFIG_FILE = "config";

    public static final String NAME_MAIN_CLASS = "mainClass";

    public static final String NAME_STATUS = "status";

    public static final String NAME_RESOURCE_ENOUGH = "resourceEnough";

    /**
     * 创建一个空任务,只包含taskId节点和状态节点
     *
     * @param taskId
     */
    public static void createEmptyTask(String taskId) {
        createEmptyTask(taskId, TaskStatus.UNALLOCATED);
    }

    public static void createEmptyTask(String taskId, TaskStatus status) {
        ZookeeperUtils.createNode(getTaskPath(taskId), CreateMode.PERSISTENT);
        ZookeeperUtils.createNodeAndSetData(getTaskPath(taskId), NAME_STATUS, status.name());
    }

    /**
     * 获得Task的config信息
     *
     * @param taskId
     * @return
     */
    public static TaskConfig getTaskConfig(String taskId) {
        return getTaskStaticInfo(taskId).getTaskConfig();
    }

    /**
     * 获得所有Task
     *
     * @return
     */
    public static List<String> getAllTasks() {
        return getChildrenWithSimplePath(TASKS);
    }

    /**
     * 获得所有当前正在处理中的任务
     *
     * @return
     */
    public static List<String> getAllProcessingTasks() {
        List<String> allTasks = getAllTasks();

        return FluentIterable.from(allTasks).filter((taskId) -> {
            TaskStatus status = TaskStorage.getStatus(taskId);
            switch (status) {
                case PROCESSING:
                    return true;
                default:
                    return false;
            }
        }).toList();
    }

    /**
     * 获得所有已结束的任务
     *
     * @return
     */
    public static List<String> getAllFinishedTasks() {
        List<String> allTasks = getAllTasks();

        return FluentIterable.from(allTasks).filter((taskId) -> {
            TaskStatus status = TaskStorage.getStatus(taskId);
            switch (status) {
                case FINISHED:
                    return true;
                default:
                    return false;
            }
        }).toList();
    }

    public static long getTaskFinishedTime(String taskId) {
        return ZookeeperUtils.getNodeStat(getTaskStatusPath(taskId)).getMtime();
    }

    /**
     * 获得最老的Task(执行完的task除外)
     *
     * @return 如不存在, 则返回null
     */
    public static String getOldestProcessingTask() {
        List<String> taskList = getAllProcessingTasks();
        if (CollectionsUtils2.isEmpty(taskList)) {
            return null;
        }

        String oldestTask = null;
        long oldestTaskZxid = Long.MAX_VALUE;
        for (String task : taskList) {
            final long czxid = getNodeStat(getTaskPath(task)).getCzxid();
            if (czxid < oldestTaskZxid) {
                oldestTaskZxid = czxid;
                oldestTask = task;
            }
        }

        return oldestTask;
    }

    /**
     * 获得未被分配执行的任务
     *
     * @return 如果不存在, 则返回null
     */
    public static String getUnallocatedTask() {
        List<String> taskList = getAllTasks();
        if (CollectionsUtils2.isEmpty(taskList)) {
            return null;
        }

        for (String task : taskList) {
            if (TaskStatus.UNALLOCATED.equals(getStatus(task))) {
                return task;
            }
        }

        return null;
    }

    /**
     * 判断该task是否存在
     *
     * @param taskId
     * @return
     */
    public static boolean taskExist(String taskId) {
        return checkExists(getTaskPath(taskId));
    }

    /**
     * 判断该task是否不存在
     *
     * @param taskId
     * @return
     */
    public static boolean taskNotExist(String taskId) {
        return !taskExist(taskId);
    }

    public static TaskStaticInfo getTaskStaticInfo(String taskId) {
        TaskStaticInfo cached = TaskStaticInfoCache.get(taskId);
        if (cached != null) {
            return cached;
        }

        ReentrantReadWriteLock lock = LockFactory.getTaskStaticInfoCacheLock(taskId);
        try {
            lock.writeLock().lock();
            // double-check
            cached = TaskStaticInfoCache.get(taskId);
            if (cached != null) {
                return cached;
            }
            TaskStaticInfo taskStaticInfo = new TaskStaticInfo(taskId);

            final String taskPath = getTaskPath(taskId);

            taskStaticInfo.setJarPath(getDataByString(makePath(taskPath, NAME_JAR)));

            String configBody = getDataByString(makePath(getTaskPath(taskId), NAME_CONFIG_FILE));
            taskStaticInfo.setTaskConfig(JSON.parseObject(configBody, TaskConfig.class));

            taskStaticInfo.setMainClass(getDataByString(makePath(taskPath, NAME_MAIN_CLASS)));

            TaskStaticInfoCache.put(taskId, taskStaticInfo);

            return taskStaticInfo;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 删除Task
     *
     * @param taskId
     */
    public static void delete(String taskId) {
        ZookeeperUtils.delete(getTaskPath(taskId));
    }

    /**
     * 获得Task的当前状态
     *
     * @param taskId
     * @return
     */
    public static TaskStatus getStatus(String taskId) {
        String statusPath = getTaskStatusPath(taskId);
        return TaskStatus.valueOf(getDataByString(statusPath));
    }

    /**
     * 获得Task的zk路径
     *
     * @param taskId
     * @return
     */
    private static String getTaskPath(String taskId) {
        return makePath(TASKS, taskId);
    }

    private static String getTaskStatusPath(String taskId) {
        return makePath(TASKS, taskId, NAME_STATUS);
    }

    /**
     * 设置运行该Task对应的jar路径
     *
     * @param taskId
     * @param jarPath
     */
    public static void setJarPath(String taskId, String jarPath) {
        ZookeeperUtils.createNodeAndSetData(getTaskPath(taskId), NAME_JAR, jarPath);
    }

    /**
     * 设置该Task对应的config信息
     *
     * @param taskId
     * @param config
     */
    public static void setConfig(String taskId, String config) {
        ZookeeperUtils.createNodeAndSetData(getTaskPath(taskId), NAME_CONFIG_FILE, config);
    }

    /**
     * 设置运行该Task的main函数
     *
     * @param taskId
     * @param mainClass
     */
    public static void setMainClass(String taskId, String mainClass) {
        ZookeeperUtils.createNodeAndSetData(getTaskPath(taskId), NAME_MAIN_CLASS, mainClass);
    }

    public static void setStatus(String taskId, TaskStatus status) {
        ZookeeperUtils.setData(makePath(getTaskPath(taskId), NAME_STATUS), status.name());
    }

    /**
     * 取消任务.
     *
     * @param taskId
     */
    public static void cancel(String taskId) {
        setStatus(taskId, TaskStatus.CANCELED);
    }

    /**
     * 得到该任务的当前运行资源是否足够的值
     *
     * @param taskId
     * @return
     */
    public static boolean isResourceEnough(String taskId) {
        String nodePath = getResourceEnoughPath(taskId);
        if (!checkExists(nodePath)) {
            return false;
        }

        return Boolean.parseBoolean(ZookeeperUtils.getDataByString(nodePath));
    }

    private static String getResourceEnoughPath(String taskId) {
        return makePath(getTaskPath(taskId), NAME_RESOURCE_ENOUGH);
    }

    /**
     * 设置该任务的当前运行资源是否足够
     *
     * @param taskId
     * @param b
     */
    public static void setResourceEnough(String taskId, boolean b) {
        String nodePath = getResourceEnoughPath(taskId);
        setData(nodePath, String.valueOf(b));
    }
}
