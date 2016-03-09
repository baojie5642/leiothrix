package xin.bluesky.leiothrix.server.action.allocate;

import org.junit.Test;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.support.RangesCreator;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author 张轲
 */
public class SequencePartitionAllocatorTest extends StorageContainerDependency {

    @Test
    public void should_find_range_sequence() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        RangesCreator creator = new RangesCreator(taskId);
        creator.createRanges();

        // when
        SequencePartitionAllocator allocator = new SequencePartitionAllocator();
        List<PartitionTask> taskList1 = allocator.findRange(taskId);
        List<PartitionTask> taskList2 = allocator.findRange(taskId);

        // then
        Set<String> rangesSet = new HashSet();
        taskList1.forEach(t -> {
            rangesSet.add(t.getPartitionRangeName());
        });
        taskList2.forEach(t->{
            rangesSet.add(t.getPartitionRangeName());
        });

        // 确保不会重新获取任务,同时也不会少获取任务
        assertThat(rangesSet.size(), is(4));
        // 再获取任务,就应该有异常抛出,表明任务都被分配完毕
        try {
            allocator.findRange(taskId);
            assert (false);
        } catch (NoTaskException e) {
            assert (true);
        }
    }
}