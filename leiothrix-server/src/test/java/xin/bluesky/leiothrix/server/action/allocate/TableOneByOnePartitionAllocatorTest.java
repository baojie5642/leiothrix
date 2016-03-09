package xin.bluesky.leiothrix.server.action.allocate;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.action.exception.WaitAndTryLaterException;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.support.RangesCreator;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author 张轲
 */
public class TableOneByOnePartitionAllocatorTest extends StorageContainerDependency {

    @Test
    public void should_find_range_one_by_one() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        RangesCreator creator = new RangesCreator(taskId);
        creator.createRanges();

        paddingTaskStaticInfo(taskId);

        // when
        TableOneByOnePartitionAllocator allocator = new TableOneByOnePartitionAllocator();
        int successCount = 0;
        try {
            List<PartitionTask> taskList1 = allocator.findRange(taskId);
            assertThat(taskList1.size(), is(2));
            assertThat(taskList1.get(0).getPartitionRangeName(), is("0-100"));
            assertThat(taskList1.get(1).getPartitionRangeName(), is("101-200"));
            successCount++;

            // 在这个地方,应该是在table-1的两个range都未处理完时,再来找第三个,即到了table-2,应该抛出异常
            allocator.findRange(taskId);
            assert (false);
        } catch (WaitAndTryLaterException e) {
            if (successCount == 1) {
                assert (true);// 这里是确认第二个findRange才抛出异常,前面两个不能抛
            } else {
                assert (false);
            }
        }
    }

    @Test
    public void should_continue_if_previous_table_finished() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        RangesCreator creator = new RangesCreator(taskId);
        creator.createRanges();

        paddingTaskStaticInfo(taskId);

        // when
        TableOneByOnePartitionAllocator allocator = new TableOneByOnePartitionAllocator();
        try {
            allocator.findRange(taskId);
            allocator.findRange(taskId);
            assert (false);
        } catch (WaitAndTryLaterException e) {
            // table-1处理完毕
            TableStorage.setStatus(taskId, "table-1", TableStatus.FINISHED);
            try {
                List<PartitionTask> newTaskList = allocator.findRange(taskId);
                assertThat(newTaskList.get(0).getTableName(), is("table-2"));
            } catch (WaitAndTryLaterException ex) {
                assert (false);//此时就不该再抛出异常了
            }
        }

    }

    @Test
    public void should_throw_no_task_exception_if_no_task_now() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        RangesCreator creator = new RangesCreator(taskId);
        creator.createRanges();

        paddingTaskStaticInfo(taskId);

        // when
        TableOneByOnePartitionAllocator allocator = new TableOneByOnePartitionAllocator();
        try {
            allocator.findRange(taskId);
            allocator.findRange(taskId);
            assert (false);
        } catch (WaitAndTryLaterException e) {
            // 将table-1删除,意味着table-1已经处理完毕了
            TableStorage.setStatus(taskId, "table-1", TableStatus.FINISHED);
            try {
                allocator.findRange(taskId);
                TableStorage.setStatus(taskId, "table-2", TableStatus.FINISHED);
                allocator.findRange(taskId);
                assert (false);
            } catch (NoTaskException ex) {
                assert (true);
            }
        }
    }

    private void paddingTaskStaticInfo(String taskId) throws IOException {
        TaskStorage.setJarPath(taskId, "/jar");
        TaskStorage.setMainClass(taskId, "MainClass");
        String taskConfig = IOUtils.toString(getClass().getResourceAsStream("/allocate/table_one_by_one_config.json"));
        TaskStorage.setConfig(taskId, taskConfig);
    }

}