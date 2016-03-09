package xin.bluesky.leiothrix.server.action;

import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;
import xin.bluesky.leiothrix.model.task.TableQueryDefinition;
import xin.bluesky.leiothrix.model.task.TaskConfig;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.action.exception.FindTaskException;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.action.exception.WaitAndTryLaterException;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.List;
import java.util.Random;

/**
 * @author 张轲
 * @date 16/1/24
 */
//todo: 关于这一次find出来的结果,都是一张表的这种做法,应该抽象出成个具体实现.因为并没有道理要求这样做.
public class PartitionTaskFinder {

    private static Random databaseRandom = new Random();

    private PartitionAllocator partitionAllocator;

    private String taskId;

    public PartitionTaskFinder(String taskId, PartitionAllocator partitionAllocator) {
        this.taskId = taskId;
        this.partitionAllocator = partitionAllocator;
    }

    public List<PartitionTask> find() throws NoTaskException, WaitAndTryLaterException {

        // 获得任务片:一张表的一个数据区间
        if (TaskStorage.taskNotExist(taskId)) {
            throw new NoTaskException(String.format("task[taskId=%s]不存在", taskId));
        }

        // 获得可供分配的任务片
        List<PartitionTask> ptList = partitionAllocator.findRange(taskId);

        // 设置该任务片需要连接的数据源
        ptList.forEach(pt -> {
            DatabaseInfo databaseInfo = chooseSuitableDatabase(taskId);
            pt.setDatabaseInfo(databaseInfo);
        });

        // 设置该任务片对应的columns,where
        List<TableQueryDefinition> tableList = TaskStorage.getTaskConfig(taskId).getTableList();
        if (!CollectionsUtils2.isEmpty(tableList)) {
            for (TableQueryDefinition definition : tableList) {
                // 一次find出来的任务片,都是一张表的
                if (definition.getName().equalsIgnoreCase(ptList.get(0).getTableName())) {
                    ptList.forEach(pt -> {
                        pt.setColumnNames(definition.getColumns());
                        pt.setWhere(definition.getWhere());
                    });
                    break;
                }
            }
        }

        return ptList;
    }

    //todo: 一个task的一张表应该尽量落到一个数据库上,以充分使用innodb_cache
    protected DatabaseInfo chooseSuitableDatabase(String taskId) {
        // 随机分配数据源,以保证压力均匀
        try {
            TaskConfig taskConfig = TaskStorage.getTaskConfig(taskId);
            List<DatabaseInfo> databaseInfoList = taskConfig.getDatabaseInfoList();
            return databaseInfoList.get(databaseRandom.nextInt(databaseInfoList.size()));
        } catch (Exception e) {
            throw new FindTaskException("选择最合适的数据源时发生异常", e);
        }
    }
}
