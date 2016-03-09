package xin.bluesky.leiothrix.server.background;

import org.junit.Before;
import org.junit.Test;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static xin.bluesky.leiothrix.server.bean.status.RangeStatus.PROCESSING;

/**
 * @author 张轲
 * @date 16/3/3
 */
public class TimeoutPartitionTaskCheckerTest extends StorageContainerDependency {

    @Before
    public void setUp() throws Exception {
        // 把超时阀值设短些,方便测试
        TimeoutPartitionTaskChecker.RANGE_UPDATED_THRESHOLD = 2;
    }

    @Test
    public void test_correct_timeout_check() throws Exception {

        String taskId = UUID.randomUUID().toString();
        TaskStorage.createEmptyTask(taskId);
        TaskStorage.setStatus(taskId, TaskStatus.PROCESSING);

        createRanges(taskId);

        // 等待一段时间,以超过阀值
        Thread.sleep(3 * 1000);

        TimeoutPartitionTaskChecker checker = new TimeoutPartitionTaskChecker();
        checker.run();
        assertThat(RangeStorage.getRangeStatus(taskId, "table-1", "0-100"), is(RangeStatus.UNALLOCATED));
    }

    private void createRanges(String taskId) {
        final String tableName = "table-1";
        final String rangeName = "0-100";
        TableMeta table1Meta = new TableMeta(tableName, "id");
        TableStorage.createTable(taskId, table1Meta);
        TableStorage.setStatus(taskId, tableName, TableStatus.PROCESSING);

        RangeStorage.createRange(taskId, tableName, rangeName);
        RangeStorage.setRangeStatus(taskId, tableName, rangeName, PROCESSING);
    }

}