package xin.bluesky.leiothrix.server.storage;

import com.alibaba.fastjson.JSON;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.bean.task.TaskTables;
import xin.bluesky.leiothrix.server.cache.TaskTablesCache;
import xin.bluesky.leiothrix.server.cache.TaskTablesMetaCache;
import xin.bluesky.leiothrix.server.lock.LockFactory;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.*;

/**
 * @author 张轲
 * @date 16/2/17
 */
public class TableStorage {
    public static final String NAME_META = "meta";

    public static final String NAME_STATUS = "status";

    public static final String NAME_TABLES = "tables";

    public static void createTable(String taskId, TableMeta tableMeta) {
        String tableName = tableMeta.getTableName();
        createNodeAndSetData(getTablePath(taskId, tableName), NAME_META, JSON.toJSONString(tableMeta));
        createNodeAndSetData(getTablePath(taskId, tableName), NAME_STATUS, TableStatus.UNALLOCATED.name());
    }

    public static TableMeta getTableMeta(String taskId, String tableName) {
        TableMeta cached = TaskTablesMetaCache.get(taskId, tableName);
        if (cached != null) {
            return cached;
        }

        ReentrantReadWriteLock lock = LockFactory.getTaskTableMetaCacheLock(taskId, tableName);
        lock.writeLock().lock();
        try {
            cached = TaskTablesMetaCache.get(taskId, tableName);
            if (cached != null) {
                return cached;
            }

            String metaPath = makePath(getTablePath(taskId, tableName), NAME_META);
            TableMeta tableMeta = JSON.parseObject(getDataByString(metaPath), TableMeta.class);
            TaskTablesMetaCache.put(taskId, tableMeta);

            return tableMeta;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static List<String> getAllTablesByTaskId(String taskId) {
        TaskTables cached = TaskTablesCache.get(taskId);
        if (cached != null) {
            return cached.getTableNameList();
        }

        ReentrantReadWriteLock lock = LockFactory.getTaskTablesCacheLock(taskId);
        lock.writeLock().lock();
        try {
            cached = TaskTablesCache.get(taskId);
            if (cached != null) {
                return cached.getTableNameList();
            }

            String tablesPath = makePath(TaskStorage.TASKS, taskId, NAME_TABLES);
            List<String> tableNameList = getChildrenWithSimplePath(tablesPath);
            TaskTablesCache.put(new TaskTables(taskId, tableNameList));

            return tableNameList;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static TableStatus getStatus(String taskId, String tableName) {
        String statusPath = getStatusPath(taskId, tableName);
        return TableStatus.valueOf(getDataByString(statusPath));
    }

    private static String getStatusPath(String taskId, String tableName) {
        return makePath(getTablePath(taskId, tableName), NAME_STATUS);
    }

    public static void setStatus(String taskId, String tableName, TableStatus status) {
        String statusPath = getStatusPath(taskId, tableName);
        ZookeeperUtils.setData(statusPath, status.name());
    }

    private static String getTablePath(String taskId, String tableName) {
        return makePath(TaskStorage.TASKS, taskId, NAME_TABLES, tableName);
    }

}
