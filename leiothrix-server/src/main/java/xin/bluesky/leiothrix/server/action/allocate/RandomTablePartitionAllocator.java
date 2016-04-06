package xin.bluesky.leiothrix.server.action.allocate;

import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;

import java.util.*;

/**
 * 按照table作为维度,随机分配.但table内部的range按id顺序依次执行
 *
 * <p>eg:假设两个表(table-1,table-2),各自range情况如下:
 * <ul>
 * <li>
 * table-1:0-1000,1001-2000两个range
 * </li>
 * <li>
 * table-2:0-3000,3001-6000,6001-9000三个range
 * </li>
 * </ul>
 * 那么分配顺序依次是:worker每次取到table-1或table-2,但table1按照0-1000,1001-2000的顺序执行,table-2按照0-3000,3001-6000,6001-9000的顺序执行
 *
 * @author 张轲
 */
@Deprecated
public class RandomTablePartitionAllocator extends AbstractPartitionAllocator {

    @Override
    public List<PartitionTask> findRange(String taskId, List<String> tableNameList) throws NoTaskException {
        final String noTaskText = String.format("任务[taskId=%s]下没有未被分配的任务片", taskId);

        List<PartitionTask> result = new ArrayList();

        Map<String, Object> noPartitionTaskTables = new HashMap();
        Random random = new Random();

        while (true) {
            if (noPartitionTaskTables.size() == tableNameList.size()) {
                throw new NoTaskException(noTaskText);
            }

            String tableName = tableNameList.get(random.nextInt(tableNameList.size()));
            if (noPartitionTaskTables.containsKey(tableName)) {
                continue;
            }

            if (isTableProcessingOrFinished(taskId, tableName)) {
                noPartitionTaskTables.put(tableName, new Object());
                continue;
            }

            //todo: 在random方式下,这样扫描给出某个表的所有scan是有问题的,导致仍然是集中处理一张表
            RangeScanner scanner = new RangeScanner(taskId, tableName);
            RangeScanResult scanResult = scanner.scan();

            if (scanResult.hasUnallocatedRange()) {
                result = preAllocate(taskId, tableName, scanResult.getUnallocatedRangeNameList());
                break;
            } else {
                noPartitionTaskTables.put(tableName, new Object());
                continue;
            }
        }

        if (result.isEmpty()) {
            throw new NoTaskException(noTaskText);
        }
        return result;

    }
}
