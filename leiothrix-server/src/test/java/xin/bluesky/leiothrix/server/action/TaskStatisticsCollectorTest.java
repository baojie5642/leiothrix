package xin.bluesky.leiothrix.server.action;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import xin.bluesky.leiothrix.model.task.partition.ExecutionStatistics;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * @author 张轲
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {TaskStorage.class, TableStorage.class, RangeStorage.class})
public class TaskStatisticsCollectorTest {

    @Test
    public void should_print_correct_workInfo() throws Exception {
        // given
        String taskId = UUID.randomUUID().toString();

        String worker1 = "192.168.100.1";
        String worker2 = "192.168.100.2";

        String p1_worker1 = "1000";
        String p2_worker1 = "1001";
        String p1_worker2 = "2000";

        // when
        PowerMockito.mockStatic(TaskStorage.class);
        when(TaskStorage.getAllTaskWorkers(taskId)).thenReturn(ImmutableList.of(worker1, worker2));
        when(TaskStorage.getWorkerProcessor(taskId, worker1)).thenReturn(ImmutableList.of(p1_worker1, p2_worker1));
        when(TaskStorage.getWorkerProcessor(taskId, worker2)).thenReturn(ImmutableList.of(p1_worker2));

        when(TaskStorage.getWorkerProcessorStartTime(taskId, worker1, p1_worker1)).thenReturn("2016-01-01 17:20:30");
        when(TaskStorage.getWorkerProcessorFinishedTime(taskId, worker1, p1_worker1)).thenReturn("2016-01-01 18:20:30");

        when(TaskStorage.getWorkerProcessorStartTime(taskId, worker1, p2_worker1)).thenReturn("2016-01-01 17:22:00");
        when(TaskStorage.getWorkerProcessorFinishedTime(taskId, worker1, p2_worker1)).thenReturn(null);

        when(TaskStorage.getWorkerProcessorStartTime(taskId, worker2, p1_worker2)).thenReturn("2016-01-01 17:23:00");
        when(TaskStorage.getWorkerProcessorFinishedTime(taskId, worker2, p1_worker2)).thenReturn("2016-01-01 17:43:00");

        TaskStatisticsCollector collector = new TaskStatisticsCollector(taskId);
        StringBuffer result = collector.collectWorkersInfo();

        System.out.println(result.toString());
    }

    @Test
    public void should_print_correct_execution_statistics() {
        // given
        String taskId = UUID.randomUUID().toString();

        String table1 = "table-1";
        String table2 = "table-2";

        String range1_table1 = "0-1000";
        String range2_table1 = "1001-2000";
        String range1_table2 = "0-10000";

        PowerMockito.mockStatic(TableStorage.class);
        when(TableStorage.getAllTablesByTaskId(taskId)).thenReturn(ImmutableList.of(table1, table2));

        PowerMockito.mockStatic(RangeStorage.class);
        when(RangeStorage.getAllRangesByTableName(taskId, table1)).thenReturn(ImmutableList.of(range1_table1, range2_table1));
        when(RangeStorage.getAllRangesByTableName(taskId, table2)).thenReturn(ImmutableList.of(range1_table2));

        when(RangeStorage.getExecutionStatistics(taskId, table1, range1_table1)).thenReturn(
                new ExecutionStatistics(100, 100, 0, null, null, 5 * 1000, 7 * 1000, 15 * 1000)
        );
        when(RangeStorage.getExecutionStatistics(taskId, table1, range2_table1)).thenReturn(
                new ExecutionStatistics(200, 150, 50, "0-100;101-200", "table1-exception", 3 * 1000, 2 * 1000, 5 * 1000)
        );
        when(RangeStorage.getExecutionStatistics(taskId, table2, range1_table2)).thenReturn(
                new ExecutionStatistics(400, 400, 0, null, null, 7 * 1000, 5 * 1000, 15 * 1000)
        );

        TaskStatisticsCollector collector = new TaskStatisticsCollector(taskId);
        StringBuffer result = collector.collectExecutionStatistics();
        System.out.println(result.toString());
    }
}