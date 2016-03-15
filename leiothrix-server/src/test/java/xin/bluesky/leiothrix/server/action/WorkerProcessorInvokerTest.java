package xin.bluesky.leiothrix.server.action;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import xin.bluesky.leiothrix.server.bean.node.NodeInfo;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static xin.bluesky.leiothrix.server.action.WorkerProcessorInvoker.WORKER_MEMORY_REDUNDANCY;
import static xin.bluesky.leiothrix.server.action.WorkerProcessorInvoker.WORKER_PROCESSOR_MEMORY;

/**
 * @author 张轲
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerStorage.class)
public class WorkerProcessorInvokerTest {

    /**
     * running=0,configured=2,physicalUpper=2,应该返回2
     */
    @Test
    public void should_cal_correct_1() {
        // given
        NodeInfo worker = createWorker("localhost",
                (WORKER_PROCESSOR_MEMORY * 2 + WORKER_MEMORY_REDUNDANCY * 2) << 10);

        // when
        PowerMockito.mockStatic(WorkerStorage.class);
        when(WorkerStorage.getWorkersNumber(anyString())).thenReturn(0);

        WorkerProcessorInvoker invoker = new WorkerProcessorInvoker();
        int result = invoker.calAvailableProcessNum(worker);

        assertThat(result, is(2));
    }

    /**
     * running=0,configured=2,physicalUpper=3,应该返回2
     */
    @Test
    public void should_cal_correct_2() {
        // given
        NodeInfo worker = createWorker("localhost",
                (WORKER_PROCESSOR_MEMORY * 3 + WORKER_MEMORY_REDUNDANCY * 2) << 10);

        // when
        PowerMockito.mockStatic(WorkerStorage.class);
        when(WorkerStorage.getWorkersNumber(anyString())).thenReturn(0);

        WorkerProcessorInvoker invoker = new WorkerProcessorInvoker();
        int result = invoker.calAvailableProcessNum(worker);

        assertThat(result, is(2));
    }

    /**
     * running=2,configured=2,physicalUpper=3,应该返回0
     */
    @Test
    public void should_cal_correct_3() {
        // given
        NodeInfo worker = createWorker("localhost",
                (WORKER_PROCESSOR_MEMORY * 3 + WORKER_MEMORY_REDUNDANCY * 2) << 10);

        // when
        PowerMockito.mockStatic(WorkerStorage.class);
        when(WorkerStorage.getWorkersNumber(anyString())).thenReturn(2);

        WorkerProcessorInvoker invoker = new WorkerProcessorInvoker();
        int result = invoker.calAvailableProcessNum(worker);

        assertThat(result, is(0));
    }

    /**
     * running=1,configured=2,physicalUpper=3,应该返回1
     */
    @Test
    public void should_cal_correct_4() {
        // given
        NodeInfo worker = createWorker("localhost",
                (WORKER_PROCESSOR_MEMORY * 3 + WORKER_MEMORY_REDUNDANCY * 2) << 10);

        // when
        PowerMockito.mockStatic(WorkerStorage.class);
        when(WorkerStorage.getWorkersNumber(anyString())).thenReturn(1);

        WorkerProcessorInvoker invoker = new WorkerProcessorInvoker();
        int result = invoker.calAvailableProcessNum(worker);

        assertThat(result, is(1));
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