package xin.bluesky.leiothrix.server.action.allocate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.task.TaskConfig;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.action.exception.WaitAndTryLaterException;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.ArrayList;
import java.util.List;

/**
 * 按照表为维度,一张表一张表地处理.
 *
 * <p>假设有两张表table-1和table-2需要处理,则需要在table-1全部处理完毕之后再处理table-2的数据(比如数据清洗),
 * 此时需要保证表之间严格有序.在table-1处理完之前,即使有空闲worker,也要等待.
 * </p>
 *
 * @author 张轲
 */
public class TableOneByOnePartitionAllocator extends AbstractPartitionAllocator {

    public static final Logger logger = LoggerFactory.getLogger(TableOneByOnePartitionAllocator.class);

    @Override
    protected List<String> getAllTables(String taskId) {
        // 从config中取能保证表的有序,即按照从前往后的配置顺序
        TaskConfig taskConfig = TaskStorage.getTaskConfig(taskId);
        List<String> tableList = taskConfig.getTableNameList();
        // 如果application没有配table列表,那就只能从zookeeper中取了,这往往意味这application配置不正确
        if (CollectionsUtils2.isEmpty(tableList)) {
            logger.warn("application没有传table列表,这样应该是不正确的!");
            return super.getAllTables(taskId);
        }
        return tableList;
    }

    /**
     * 这种场景的分配调度比较复杂.
     * 在不考虑tableStatus为WAIT_FOR_REALLOCATE的情况下:
     * 1. 如表状态为已结束,则可进入下一张表去分配range
     * 2. 如表状态为处理中,则抛出{@link WaitAndTryLaterException},通知worker等待该表处理完成
     * 3. 如表状态为未分配,则进入.此时所有range都是未分配的(因为取未分配的任务片的原则是一次性将table下的所有未分配range都取出来),所以一定能够找到未分配range来返回
     *
     * 考虑tableStatus为WAIT_FOR_REALLOCATE的情况下,此时该表下可能部分range是已结束,部分range是处理中,
     * 但一定有range是未分配的,因为在判定range处理超时的时候才会给table置WAIT_FOR_REALLOCATE状态,同时重置超时range为未分配状态.
     *
     * @param taskId
     * @param tableNameList
     * @return
     * @throws NoTaskException
     * @throws WaitAndTryLaterException
     */
    @Override
    public List<PartitionTask> findRange(String taskId, List<String> tableNameList) throws NoTaskException, WaitAndTryLaterException {
        List<PartitionTask> result = new ArrayList();

        for (int i = 0; i < tableNameList.size(); i++) {
            String tableName = tableNameList.get(i);

            switch (TableStorage.getStatus(taskId, tableName)) {
                case FINISHED:
                    break;
                case PROCESSING:
                    if (i == tableNameList.size() - 1) {//如果是最后一张表,则无需worker再等待
                        throw new NoTaskException();
                    } else {//该表尚未结束,通知worker等待并稍后尝试
                        throw new WaitAndTryLaterException();
                    }
                case UNALLOCATED:
                case WAIT_FOR_REALLOCATE:
                    RangeScanner scanner = new RangeScanner(taskId, tableName);
                    RangeScanResult scanResult = scanner.scan();
                    result = preAllocate(taskId, tableName, scanResult.getUnallocatedRangeNameList());
                    break;
                default://这不应该发生
                    throw new NoTaskException();
            }

            if (!result.isEmpty()) {
                return result;
            }
        }

        // 如果执行到这还没有未分配任务片,则表明所有table都是FINISHED状态了,抛出NoTask的异常
        throw new NoTaskException();

    }
}
