package xin.bluesky.leiothrix.server.action.allocate;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;

import java.util.*;

/**
 * @author 张轲
 */
@Ignore(value = "类并不支持,测试暂且忽略")
public class RandomTablePartitionAllocatorTest extends StorageContainerDependency {

    private static final Logger logger = LoggerFactory.getLogger(RandomTablePartitionAllocator.class);

    @Test
    public void should_find_range_by_random() throws Exception {
        // given 为了测试随机的效率,这里创建较多的表和range
        String taskId = UUID.randomUUID().toString();
        int totalRangeNumber = createManyRanges(taskId);

        // when
        RandomTablePartitionAllocator allocator = new RandomTablePartitionAllocator();
        Map<String, Integer> map = new HashMap();
        synchronized (this) {
            for (int i = 0; i < totalRangeNumber; i++) {
                List<PartitionTask> list = allocator.findRange(taskId);
                list.forEach(partitionTask -> {
                    String key = partitionTask.getTableName() + ":" + partitionTask.getRangeName();
                    if (map.containsKey(key)) {
                        map.put(key, map.get(key) + 1);
                    } else {
                        map.put(key, 1);
                    }
                });

            }
        }
        assert (true);//应该这所有任务片都能分到

        try {
            allocator.findRange(taskId);
            assert (false);//这时候就不应该再分到了,而是抛出异常
        } catch (NoTaskException e) {
            assert (true);
        }

        if (map.entrySet().size() != totalRangeNumber) {
            assert (false);
        }
        //如果有range被重复分配,则测试不通过
        for (Iterator<Integer> values = map.values().iterator(); values.hasNext(); ) {
            if (values.next() != 1) {
                assert (false);
            }
        }
    }

    private int createManyRanges(String taskId) {
        int tableNumber = 10;
        int rangeNumberPerTable = 20;

        for (int i = 0; i < tableNumber; i++) {
            final String tableName = "table-" + i;
            TableMeta tableMeta = new TableMeta(tableName, "id");
            TableStorage.createTable(taskId, tableMeta);

            for (int j = 0; j < rangeNumberPerTable; j++) {
                final int rangeScope = 10000;
                RangeStorage.createRange(taskId, tableName, rangeScope * j + "-" + rangeScope * (j + 1));
            }
        }

        int totalRangeNumber = rangeNumberPerTable * tableNumber;
        logger.info("成功创建{}个range", totalRangeNumber);
        return totalRangeNumber;
    }

}