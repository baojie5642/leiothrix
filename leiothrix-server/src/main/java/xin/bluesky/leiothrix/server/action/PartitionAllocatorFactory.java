package xin.bluesky.leiothrix.server.action;

import xin.bluesky.leiothrix.model.task.TaskConfig;
import xin.bluesky.leiothrix.server.action.allocate.RandomTablePartitionAllocator;
import xin.bluesky.leiothrix.server.action.allocate.SequencePartitionAllocator;
import xin.bluesky.leiothrix.server.action.allocate.TableOneByOnePartitionAllocator;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 张轲
 */
public class PartitionAllocatorFactory {

    private static final Map<String, PartitionAllocator> map = new HashMap();

    private static final String TABLE_SEQUENCE = "table_sequence";

    public static final String RANGE_SEQUENCE = "range_sequence";

    public static final String RANDOM_TABLE = "random_table";

    static {
        map.put(TABLE_SEQUENCE, new TableOneByOnePartitionAllocator());
        map.put(RANGE_SEQUENCE, new SequencePartitionAllocator());
        map.put(RANDOM_TABLE, new RandomTablePartitionAllocator());
    }

    /**
     * 根据task的配置,来获得对应的{@link PartitionAllocator}的实现类
     *
     * @param taskId taskId
     * @return {@link PartitionAllocator} object
     */
    public static PartitionAllocator get(String taskId) {
        TaskConfig taskConfig = TaskStorage.getTaskConfig(taskId);
        PartitionAllocator allocator = map.get(taskConfig.getRangeAllocator());
        return allocator == null ? map.get(RANGE_SEQUENCE) : allocator;
    }
}
