package xin.bluesky.leiothrix.server.action.allocate;

import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;

import java.util.ArrayList;
import java.util.List;

/**
 * 按照table+range作为维度,依次分配.
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
 * 那么分配顺序依次是:table-1:0-1000,table-1:1001-2000,table-2:0-3000,table-2:3001-6000,table-3:6001-9000.
 * 当然这是理论上的划分,实际上由于多线程的关系和网络时延区别,这个顺序会略有颠倒,并不保证严格有序,但总体来说,是按照从前到后的顺序.
 *
 * <p>
 * 在这种情况下,会出现同一时刻,两个表的range在同时处理的情况,比如table-1:1001-2000和table-2:0-3000会分别被两个worker线程执行.
 * 在实际场景中,可能有这样的业务:需要等待table-1全部处理完毕,再处理table-2的数据(比如数据清洗),此时需要保证table-2严格在table-1
 * 完全结束后再执行,此时应该使用{@link TableOneByOnePartitionAllocator}
 *
 * @author 张轲
 */
public class SequencePartitionAllocator extends AbstractPartitionAllocator {

    @Override
    public List<PartitionTask> findRange(final String taskId, List<String> tableNameList) throws NoTaskException {
        List<PartitionTask> result = new ArrayList();
        for (final String tableName : tableNameList) {

            if (isTableProcessingOrFinished(taskId, tableName)) {
                continue;
            }

            RangeScanner scanner = new RangeScanner(taskId, tableName);
            RangeScanResult scanResult = scanner.scan();
            if (scanResult.hasUnallocatedRange()) {
                result = preAllocate(taskId, tableName, scanResult.getUnallocatedRangeNameList());
                break;
            } else {
                continue;
            }
        }

        if (result.isEmpty()) {
            throw new NoTaskException(String.format("任务[taskId=%s]下没有未被分配的任务片", taskId));
        }
        return result;
    }
}
