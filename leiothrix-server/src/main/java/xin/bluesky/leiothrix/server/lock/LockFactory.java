package xin.bluesky.leiothrix.server.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperClientFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author 张轲
 */
public class LockFactory {

    /**
     * 用于任务存储时使用的锁集合.由于在{@link xin.bluesky.leiothrix.server.background.TaskStatusChecker}中会删除完成的任务片以及任务,造成并发时存在虚读的现象,所以需要锁机制来保证同步
     */
    private static final Map<String, ReentrantReadWriteLock> taskStaticInfoCacheLocks = new ConcurrentHashMap<>();

    private static final Map<String, ReentrantReadWriteLock> taskTablesCacheLock = new ConcurrentHashMap<>();

    private static final Map<String, ReentrantReadWriteLock> taskTablesMetaCacheLock = new ConcurrentHashMap<>();

    private static final Map<String, ReentrantReadWriteLock> taskRangesCacheLock = new ConcurrentHashMap<>();

    public static final String PARTITION_TASK_FIND_LOCK_PATH = Constant.ROOT_DIR + "/locks";

    public static ReentrantReadWriteLock getTaskStaticInfoCacheLock(String taskId) {
        return getLock(taskId, taskStaticInfoCacheLocks);
    }

    private static ReentrantReadWriteLock getLock(String key, Map<String, ReentrantReadWriteLock> lockMap) {
        ReentrantReadWriteLock lock = lockMap.get(key);
        if (lock == null) {
            final ReentrantReadWriteLock newLock = new ReentrantReadWriteLock();
            lock = lockMap.putIfAbsent(key, newLock);
            if (lock == null) {
                lock = newLock;
            }
        }
        return lock;
    }

    public static ReentrantReadWriteLock getTaskTablesCacheLock(String taskId) {
        return getLock(taskId, taskTablesCacheLock);
    }

    public static ReentrantReadWriteLock getTaskTableMetaCacheLock(String taskId, String tableName) {
        return getLock(StringUtils2.append(taskId, ":", tableName), taskTablesMetaCacheLock);
    }

    public static ReentrantReadWriteLock getTaskRangesCacheLock(String taskId, String tableName) {
        return getLock(StringUtils2.append(taskId, ":", tableName), taskRangesCacheLock);
    }

    public static InterProcessMutex getPartitionTaskFindLock(String taskId) {
        CuratorFramework client = ZookeeperClientFactory.get();
        InterProcessMutex mutex = new InterProcessMutex(client,
                StringUtils2.append(PARTITION_TASK_FIND_LOCK_PATH, "/", taskId, "/taskfind"));
        return mutex;
    }
}
