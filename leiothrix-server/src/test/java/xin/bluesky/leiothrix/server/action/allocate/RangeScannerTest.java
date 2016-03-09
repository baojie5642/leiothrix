package xin.bluesky.leiothrix.server.action.allocate;

import org.junit.Test;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static xin.bluesky.leiothrix.server.bean.status.RangeStatus.*;

/**
 * @author 张轲
 */
public class RangeScannerTest extends StorageContainerDependency {

    @Test
    public void should_correct_if_all_finished() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        createRanges(taskId);
        RangeStorage.setRangeStatus(taskId, "table-1", "0-100", FINISHED);
        RangeStorage.setRangeStatus(taskId, "table-1", "101-200", FINISHED);

        // when
        RangeScanner scanner = new RangeScanner(taskId, "table-1");
        RangeScanResult result = scanner.scan();

        // then
        assertThat(result.hasUnallocatedRange(), is(false));
        assertThat(result.isTableFinished(), is(true));
        assertThat(result.getUnallocatedRangeNameList().size(), is(0));
        TableStatus tableStatus = TableStorage.getStatus(taskId, "table-1");
        assertThat(tableStatus, is(TableStatus.FINISHED));
    }

    @Test
    public void should_correct_if_has_unallocated() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        createRanges(taskId);
        RangeStorage.setRangeStatus(taskId, "table-1", "0-100", UNALLOCATED);
        RangeStorage.setRangeStatus(taskId, "table-1", "101-200", FINISHED);

        // when
        RangeScanner scanner = new RangeScanner(taskId, "table-1");
        RangeScanResult result = scanner.scan();

        // then
        assertThat(result.hasUnallocatedRange(), is(true));
        assertThat(result.isTableFinished(), is(false));
        assertThat(result.getUnallocatedRangeNameList().get(0), is("0-100"));
        TableStatus tableStatus = TableStorage.getStatus(taskId, "table-1");
        assertThat(tableStatus, not(TableStatus.FINISHED));
    }

    @Test
    public void should_correct_if_all_processing() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        createRanges(taskId);
        RangeStorage.setRangeStatus(taskId, "table-1", "0-100", PROCESSING);
        RangeStorage.setRangeStatus(taskId, "table-1", "101-200", PROCESSING);

        // when
        RangeScanner scanner = new RangeScanner(taskId, "table-1");
        RangeScanResult result = scanner.scan();

        // then
        assertThat(result.hasUnallocatedRange(), is(false));
        assertThat(result.isTableFinished(), is(false));
        assertThat(result.getUnallocatedRangeNameList().size(), is(0));
    }

    @Test
    public void should_correct_if_some_processing() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();
        createRanges(taskId);
        RangeStorage.setRangeStatus(taskId, "table-1", "0-100", PROCESSING);
        RangeStorage.setRangeStatus(taskId, "table-1", "101-200", FINISHED);

        // when
        RangeScanner scanner = new RangeScanner(taskId, "table-1");
        RangeScanResult result = scanner.scan();

        // then
        assertThat(result.hasUnallocatedRange(), is(false));
        assertThat(result.isTableFinished(), is(false));
        assertThat(result.getUnallocatedRangeNameList().size(), is(0));
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