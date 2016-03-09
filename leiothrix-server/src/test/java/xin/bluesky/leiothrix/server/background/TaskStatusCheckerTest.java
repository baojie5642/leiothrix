package xin.bluesky.leiothrix.server.background;

import org.junit.Test;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;
import xin.bluesky.leiothrix.model.task.TaskStatus;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author 张轲
 */
public class TaskStatusCheckerTest extends StorageContainerDependency {

    @Test
    public void test_correct_status_check() throws Exception {

        String taskId = UUID.randomUUID().toString();
        TaskStorage.createEmptyTask(taskId);
        createRanges(taskId);

        // 第一次跑的时候,什么状态都不会改变
        TaskStatusChecker checker = new TaskStatusChecker();
        checker.run();
        assertThat(TaskStorage.getStatus(taskId), is(TaskStatus.UNALLOCATED));
        assertThat(TableStorage.getStatus(taskId, "table-1"), is(TableStatus.UNALLOCATED));
        assertThat(RangeStorage.getRangeStatus(taskId, "table-1", "0-100"), is(RangeStatus.UNALLOCATED));

        // 当table-1的range1为结束状态,table-1和task都应该还是处理中状态
        TableStorage.setStatus(taskId, "table-1", TableStatus.PROCESSING);
        TaskStorage.setStatus(taskId, TaskStatus.PROCESSING);
        RangeStorage.setRangeStatus(taskId, "table-1", "0-100", RangeStatus.FINISHED);
        checker.run();
        assertThat(TaskStorage.getStatus(taskId), is(TaskStatus.PROCESSING));
        assertThat(TableStorage.getStatus(taskId, "table-1"), is(TableStatus.PROCESSING));

        // 再给第二个range置FINISH状态,即该表下所有range都已结束了,验证table的状态也应该为FINISHED
        RangeStorage.setRangeStatus(taskId, "table-1", "101-200", RangeStatus.FINISHED);
        checker.run();
        assertThat(TableStorage.getStatus(taskId, "table-1"), is(TableStatus.FINISHED));
        assertThat(TaskStorage.getStatus(taskId), is(TaskStatus.PROCESSING));

        // 再把第二个表的range都设置为FINISH状态,验证task的状态也应该为FINISHED
        TableStorage.setStatus(taskId, "table-2", TableStatus.PROCESSING);
        RangeStorage.setRangeStatus(taskId, "table-2", "0-1000", RangeStatus.FINISHED);
        RangeStorage.setRangeStatus(taskId, "table-2", "1001-2000", RangeStatus.FINISHED);
        checker.run();
        assertThat(TableStorage.getStatus(taskId, "table-2"), is(TableStatus.FINISHED));
        assertThat(TaskStorage.getStatus(taskId), is(TaskStatus.FINISHED));
    }

    private void createRanges(String taskId) {
        TableMeta table1Meta = new TableMeta("table-1", "id");
        TableMeta table2Meta = new TableMeta("table-2", "id");

        TableStorage.createTable(taskId, table1Meta);
        TableStorage.createTable(taskId, table2Meta);

        RangeStorage.createRange(taskId, "table-1", "0-100");
        RangeStorage.createRange(taskId, "table-1", "101-200");
        RangeStorage.createRange(taskId, "table-2", "0-1000");
        RangeStorage.createRange(taskId, "table-2", "1001-2000");
    }
}