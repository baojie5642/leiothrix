package xin.bluesky.leiothrix.server.storage;

import org.junit.Test;
import xin.bluesky.leiothrix.model.task.partition.ExecutionStatistics;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author 张轲
 */
public class RangeStorageTest extends StorageContainerDependency {

    @Test
    public void should_operate_execution_statistics_correct() throws Exception {
        // given
        String taskId = "taskId";
        String tableName = "tableName";
        String rangeName = "rangeNaem";
        ExecutionStatistics page1Stat = new ExecutionStatistics(200, 150, 50, "0-50", "page1-exception", 10 * 1000, 20 * 1000, 40 * 1000);
        ExecutionStatistics page2Stat = new ExecutionStatistics(100, 80, 20, "0-20", "page2-exception", 10 * 1000, 20 * 1000, 40 * 1000);

        // when
        RangeStorage.updatePageExecutionStatistics(taskId, tableName, rangeName, page1Stat);
        RangeStorage.updatePageExecutionStatistics(taskId, tableName, rangeName, page2Stat);
        ExecutionStatistics result = RangeStorage.getExecutionStatistics(taskId, tableName, rangeName);

        assertThat(result.getHandledRecordNum(), is(300));
        assertThat(result.getSuccessRecordNum(), is(230));
        assertThat(result.getFailRecordNum(), is(70));
        assertThat(result.getFailPageName(), is("0-50;0-20;"));
        assertThat(result.getExceptionStackTrace(), is("page1-exception\r\npage2-exception\r\n"));
        assertThat(result.getTotalTime(), is(80 * 1000l));
        assertThat(result.getQueryUsingTime(), is(20 * 1000l));
        assertThat(result.getHandleUsingTime(), is(40 * 1000l));
    }

    @Test
    public void should_operate_execution_statistics_correct_if_success() throws Exception {
        // given
        String taskId = "taskId";
        String tableName = "tableName";
        String rangeName = "rangeNaem";
        ExecutionStatistics page1Stat = new ExecutionStatistics(200, 200, 0, null, null, 10 * 1000, 20 * 1000, 40 * 1000);
        ExecutionStatistics page2Stat = new ExecutionStatistics(100, 100, 0, null, null, 10 * 1000, 20 * 1000, 40 * 1000);

        // when
        RangeStorage.updatePageExecutionStatistics(taskId, tableName, rangeName, page1Stat);
        RangeStorage.updatePageExecutionStatistics(taskId, tableName, rangeName, page2Stat);
        ExecutionStatistics result = RangeStorage.getExecutionStatistics(taskId, tableName, rangeName);

        assertThat(result.getHandledRecordNum(), is(300));
        assertThat(result.getSuccessRecordNum(), is(300));
        assertThat(result.getFailRecordNum(), is(0));
        assertThat(result.getFailPageName(), nullValue());
        assertThat(result.getExceptionStackTrace(), nullValue());
        assertThat(result.getTotalTime(), is(80 * 1000l));
        assertThat(result.getQueryUsingTime(), is(20 * 1000l));
        assertThat(result.getHandleUsingTime(), is(40 * 1000l));
    }

}