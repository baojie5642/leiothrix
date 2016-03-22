package xin.bluesky.leiothrix.worker.background;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import xin.bluesky.leiothrix.common.util.PhysicalUtils;
import xin.bluesky.leiothrix.worker.WorkerProcessor;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author 张轲
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PhysicalUtils.class, WorkerProcessor.class})
public class ResourceMonitorTest {

    @Test
    public void should_reduce_pressure_if_cpu_overload() throws Exception {

        WorkerProcessor processor = Mockito.mock(WorkerProcessor.class);
        when(processor.reducePressure()).thenReturn(true);

        PowerMockito.mockStatic(WorkerProcessor.class);
        when(WorkerProcessor.getProcessor()).thenReturn(processor);

        PowerMockito.mockStatic(PhysicalUtils.class);
        when(PhysicalUtils.getCpuLoad()).thenReturn(new BigDecimal(0.95));

        ResourceMonitor monitor = new ResourceMonitor();
        monitor.setWaitTimeAfterReducePressure(1);
        monitor.run();
        assertThat(monitor.getCpuWarnCount(), is(1));
        monitor.run();
        monitor.run();

        verify(processor).reducePressure();
        assertThat(monitor.getCpuWarnCount(), is(0));
    }

    @Test
    public void should_clear_cpu_count_if_cpu_return_normal() throws Exception {

        WorkerProcessor processor = Mockito.mock(WorkerProcessor.class);
        when(processor.reducePressure()).thenReturn(true);

        PowerMockito.mockStatic(WorkerProcessor.class);
        when(WorkerProcessor.getProcessor()).thenReturn(processor);

        PowerMockito.mockStatic(PhysicalUtils.class);
        when(PhysicalUtils.getCpuLoad()).thenReturn(new BigDecimal(0.95));

        ResourceMonitor monitor = new ResourceMonitor();
        monitor.setWaitTimeAfterReducePressure(1);
        monitor.run();
        assertThat(monitor.getCpuWarnCount(), is(1));
        monitor.run();
        when(PhysicalUtils.getCpuLoad()).thenReturn(new BigDecimal(0.8));
        monitor.run();

        assertThat(monitor.getCpuWarnCount(), is(0));
    }

    @Test
    public void should_reduce_pressure_if_gc_times_overload() throws Exception {

        WorkerProcessor processor = Mockito.mock(WorkerProcessor.class);
        when(processor.reducePressure()).thenReturn(true);

        PowerMockito.mockStatic(WorkerProcessor.class);
        when(WorkerProcessor.getProcessor()).thenReturn(processor);

        PowerMockito.mockStatic(PhysicalUtils.class);
        when(PhysicalUtils.getCpuLoad()).thenReturn(new BigDecimal(0.8));

        ResourceMonitor monitor = new ResourceMonitor();
        monitor.setWaitTimeAfterReducePressure(1);

        when(PhysicalUtils.getOldGcTimes()).thenReturn(1l);
        monitor.run();
        when(PhysicalUtils.getOldGcTimes()).thenReturn(2l);
        monitor.run();
        assertThat(monitor.getGcWarnCount(), is(1));
        when(PhysicalUtils.getOldGcTimes()).thenReturn(3l);
        monitor.run();

        verify(processor).reducePressure();
        assertThat(monitor.getGcWarnCount(), is(0));
    }

    @Test
    public void should_clear_gc_count_if_gc_return_normal() throws Exception {

        WorkerProcessor processor = Mockito.mock(WorkerProcessor.class);
        when(processor.reducePressure()).thenReturn(true);

        PowerMockito.mockStatic(WorkerProcessor.class);
        when(WorkerProcessor.getProcessor()).thenReturn(processor);

        PowerMockito.mockStatic(PhysicalUtils.class);
        when(PhysicalUtils.getCpuLoad()).thenReturn(new BigDecimal(0.8));

        ResourceMonitor monitor = new ResourceMonitor();
        monitor.setWaitTimeAfterReducePressure(1);

        when(PhysicalUtils.getOldGcTimes()).thenReturn(1l);
        monitor.run();
        when(PhysicalUtils.getOldGcTimes()).thenReturn(2l);
        monitor.run();
        assertThat(monitor.getGcWarnCount(), is(1));
        when(PhysicalUtils.getOldGcTimes()).thenReturn(2l);
        monitor.run();

        assertThat(monitor.getGcWarnCount(), is(0));
    }
}