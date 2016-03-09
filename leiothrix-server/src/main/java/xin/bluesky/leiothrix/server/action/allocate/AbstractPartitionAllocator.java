package xin.bluesky.leiothrix.server.action.allocate;

import com.google.common.collect.FluentIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.server.action.PartitionAllocator;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.action.exception.WaitAndTryLaterException;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;

import java.util.List;

/**
 * @author 张轲
 */
public abstract class AbstractPartitionAllocator implements PartitionAllocator {

    private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionAllocator.class);

    @Override
    public List<PartitionTask> findRange(String taskId) throws NoTaskException, WaitAndTryLaterException {
        List<String> tableNameList = getAllTables(taskId);

        if (CollectionsUtils2.isEmpty(tableNameList)) {
            throw new NoTaskException(String.format("没有找到任务[taskId={}]的表", taskId));
        }

        return findRange(taskId, tableNameList);
    }

    protected List<String> getAllTables(String taskId) {
        return TableStorage.getAllTablesByTaskId(taskId);
    }

    protected abstract List<PartitionTask> findRange(String taskId, List<String> tableNameList) throws NoTaskException, WaitAndTryLaterException;

    protected List<PartitionTask> preAllocate(String taskId, String tableName, List<String> rangeNameList) {
        List<PartitionTask> result = FluentIterable.from(rangeNameList)
                .transform((rangeName) -> {
                    PartitionTask partitionTask = RangeStorage.getPartitionTask(taskId, tableName, rangeName);
                    RangeStorage.setRangeStatus(taskId, tableName, rangeName, RangeStatus.PRE_ALLOCATE);
                    return partitionTask;
                })
                .toList();

        TableStorage.setStatus(taskId, tableName, TableStatus.PROCESSING);

        return result;
    }

    protected boolean isTableProcessingOrFinished(String taskId, String tableName) {
        TableStatus status = TableStorage.getStatus(taskId, tableName);
        return status == TableStatus.FINISHED || status == TableStatus.PROCESSING;
    }

    protected boolean isTableProcessing(String taskId, String tableName) {
        TableStatus status = TableStorage.getStatus(taskId, tableName);
        return status == TableStatus.PROCESSING;
    }

    protected boolean isTableFinished(String taskId, String tableName) {
        TableStatus status = TableStorage.getStatus(taskId, tableName);
        return status == TableStatus.FINISHED;
    }

}
