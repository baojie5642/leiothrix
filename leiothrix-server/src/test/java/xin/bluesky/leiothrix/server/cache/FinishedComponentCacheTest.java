package xin.bluesky.leiothrix.server.cache;

import org.junit.Test;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static xin.bluesky.leiothrix.server.bean.status.RangeStatus.FINISHED;
import static xin.bluesky.leiothrix.server.bean.status.RangeStatus.PROCESSING;

/**
 * @author 张轲
 */
public class FinishedComponentCacheTest extends StorageContainerDependency {

    @Test
    public void test_judge_correct() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        final String tableName = "table-1";
        final String rangeName = "0-100";

        TaskStorage.createEmptyTask(taskId);
        createRanges(taskId, tableName, rangeName);

        // 都是处理中状态
        TaskStorage.setStatus(taskId, TaskStatus.PROCESSING);
        TableStorage.setStatus(taskId, tableName, TableStatus.PROCESSING);
        RangeStorage.setRangeStatus(taskId, tableName, rangeName, PROCESSING);

        FinishedComponentCache cache = FinishedComponentCache.getInstance();

        assertThat(cache.isPartitionTaskFinished(taskId, tableName, rangeName), is(false));
        assertThat(cache.isTableFinished(taskId, tableName), is(false));
        assertThat(cache.isTaskFinished(taskId), is(false));

        // 都是结束状态
        TaskStorage.setStatus(taskId, TaskStatus.FINISHED);
        TableStorage.setStatus(taskId, tableName, TableStatus.FINISHED);
        RangeStorage.setRangeStatus(taskId, tableName, rangeName, FINISHED);

        assertThat(cache.isPartitionTaskFinished(taskId, tableName, rangeName), is(true));
        assertThat(cache.isTableFinished(taskId, tableName), is(true));
        assertThat(cache.isTaskFinished(taskId), is(true));
    }

    private void createRanges(String taskId, String tableName, String rangeName) {
        TableMeta table1Meta = new TableMeta(tableName, "id");
        TableStorage.createTable(taskId, table1Meta);
        RangeStorage.createRange(taskId, tableName, rangeName);
    }
}