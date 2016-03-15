package xin.bluesky.leiothrix.server.action;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import xin.bluesky.leiothrix.model.task.TaskStaticInfo;
import xin.bluesky.leiothrix.server.action.exception.NoResourceException;
import xin.bluesky.leiothrix.server.action.exception.WorkerProcessorLaunchException;
import xin.bluesky.leiothrix.server.bean.node.NodeInfo;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * @author 张轲
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskStorage.class)
public class WorkerProcessorLauncherTest {

    private WorkerProcessorLauncher launcher;

    @Mock
    private WorkerProcessorInvoker workerProcessorInvoker;

    @Mock
    private WorkerManager workerManager;

    /**
     * ***********************测试计算可用的进程数**********************************************
     */
    @Test
    public void should_cal_available_process_num_correct() throws Exception {
        // given
        String workerIp = "localhost";
        NodeInfo worker = createWorker(workerIp, 10240000);

        // when
        PowerMockito.mockStatic(TaskStorage.class);
        when(TaskStorage.getTaskStaticInfo(anyString())).thenReturn(new TaskStaticInfo("1"));
        when(workerProcessorInvoker.calAvailableProcessNum(worker)).thenReturn(1);
        when(workerManager.getAllWorkerInfoWithInitialMemory()).thenReturn(ImmutableList.of(worker));
        launcher = new WorkerProcessorLauncher("1");
        launcher.setWorkerManager(workerManager);
        launcher.setWorkerProcessorInvoker(workerProcessorInvoker);

        LaunchLog launchLog = new LaunchLog();
        launchLog.setAvailableProcessNumBeforeLuanch(workerIp, 2);
        launchLog.incProcessorNum(workerIp);
        launchLog.incProcessorNum(workerIp);

        // then
        int result = launcher.calAvailableProcessNum(worker);
        assertThat(result, is(0));
    }

    /**
     * ***********************测试启动**********************************************
     */
    @Test
    public void should_launch_correct() throws Exception {
        // given
        String workerIp1 = "192.168.100.1";
        NodeInfo worker1 = createWorker(workerIp1, 10240000);
        String workerIp2 = "192.168.100,2";
        NodeInfo worker2 = createWorker(workerIp2, 20480000);
        String taskId = UUID.randomUUID().toString();

        // when
        PowerMockito.mockStatic(TaskStorage.class);
        Mockito.when(TaskStorage.getTaskStaticInfo(anyString())).thenReturn(new TaskStaticInfo("1"));
        when(workerManager.getAllWorkerInfoWithInitialMemory()).thenReturn(ImmutableList.of(worker1, worker2));
        when(workerProcessorInvoker.calAvailableProcessNum(worker1)).thenReturn(1);
        when(workerProcessorInvoker.calAvailableProcessNum(worker2)).thenReturn(2);
        launcher = new WorkerProcessorLauncher("1");
        launcher.setWorkerManager(workerManager);
        launcher.setWorkerProcessorInvoker(workerProcessorInvoker);

        LaunchLog log = launcher.launch();
        assertThat(log.getTotalProcessorNum(), is(3));
        assertThat(log.getProcessorNum(workerIp1), is(1));
        assertThat(log.getProcessorNum(workerIp2), is(2));
    }

    @Test(expected = NoResourceException.class)
    public void should_throw_no_resource_exception_if_no_resouce() throws Exception {
        // given
        String workerIp1 = "192.168.100.1";
        NodeInfo worker1 = createWorker(workerIp1, 10240000);
        String taskId = UUID.randomUUID().toString();

        // when
        PowerMockito.mockStatic(TaskStorage.class);
        Mockito.when(TaskStorage.getTaskStaticInfo(anyString())).thenReturn(new TaskStaticInfo("1"));
        when(workerManager.getAllWorkerInfoWithInitialMemory()).thenReturn(ImmutableList.of(worker1));
        when(workerProcessorInvoker.calAvailableProcessNum(worker1)).thenReturn(0);
        launcher = new WorkerProcessorLauncher("1");
        launcher.setWorkerManager(workerManager);
        launcher.setWorkerProcessorInvoker(workerProcessorInvoker);

        launcher.launch();
    }

    @Test(expected = WorkerProcessorLaunchException.class)
    public void should_throw_launch_exception_if_launch_fail() throws Exception {
        // given
        String workerIp1 = "192.168.100.1";
        NodeInfo worker1 = createWorker(workerIp1, 10240000);
        String taskId = UUID.randomUUID().toString();

        // when
        PowerMockito.mockStatic(TaskStorage.class);
        Mockito.when(TaskStorage.getTaskStaticInfo(anyString())).thenReturn(new TaskStaticInfo("1"));
        when(workerManager.getAllWorkerInfoWithInitialMemory()).thenReturn(ImmutableList.of(worker1));
        when(workerProcessorInvoker.calAvailableProcessNum(worker1)).thenReturn(1);
        doThrow(new WorkerProcessorLaunchException()).when(workerProcessorInvoker).invoke(anyString(), anyString(), anyString(), anyString());
        launcher = new WorkerProcessorLauncher("1");
        launcher.setWorkerManager(workerManager);
        launcher.setWorkerProcessorInvoker(workerProcessorInvoker);

        launcher.launch();
    }

    private NodeInfo createWorker(String ip, long memoryFreeBeforeWorker) {
        NodePhysicalInfo physicalInfo = new NodePhysicalInfo();
        final String workerIp = ip;
        physicalInfo.setIp(workerIp);
        physicalInfo.setMemoryFreeBeforeAsWorker(memoryFreeBeforeWorker);

        NodeInfo worker = new NodeInfo();
        worker.setPhysicalInfo(physicalInfo);
        return worker;
    }
}