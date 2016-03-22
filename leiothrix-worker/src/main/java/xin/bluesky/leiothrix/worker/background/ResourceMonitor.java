package xin.bluesky.leiothrix.worker.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.PhysicalUtils;
import xin.bluesky.leiothrix.worker.WorkerProcessor;

import java.math.BigDecimal;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.math.BigDecimal.ROUND_HALF_DOWN;

/**
 * @author 张轲
 */
public class ResourceMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ResourceMonitor.class);

    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("resource-monitor").build());

    private static int MONITOR_INTERVAL = 5;

    private static String CPU_WARN_THRESHOLD = "90";

    private static int CPU_OVERLOAD_TIMES_THRESHOLD = 3;

    private static int OLD_GC_OVERLOAD_TIMES_THRESHOLD = 2;

    private int cpuWarnCount = 0;

    private int gcWarnCount = 0;

    private long lastOldGcTimes = 0;

    private int waitTimeAfterReducePressure = 30;//单位为秒

    /**
     * 是否要监听的标志位.在降压阶段不监听
     */
    private boolean monitor = true;

    public void start() {
        executor.scheduleWithFixedDelay(this, 30, MONITOR_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        if (!monitor) {
            return;
        }
        // 检查CPU来判定是否CPU过载
        BigDecimal cpuLoad = PhysicalUtils.getCpuLoad().multiply(new BigDecimal(100)).setScale(2, ROUND_HALF_DOWN);
        logger.debug("当前CPU Load:{}", cpuLoad.doubleValue());

        if (cpuLoad.compareTo(new BigDecimal(CPU_WARN_THRESHOLD)) > 0) {
            cpuWarnCount++;
        } else {
            cpuWarnCount = 0;
        }
        if (cpuWarnCount >= CPU_OVERLOAD_TIMES_THRESHOLD) {
            logger.warn("CPU Load已经连续{}次(每次间隔{}秒)超过了警戒值[{}],需要降低压力",
                    CPU_OVERLOAD_TIMES_THRESHOLD, MONITOR_INTERVAL, CPU_WARN_THRESHOLD);
            reducePressure();
            cpuWarnCount = 0;
            return;
        }

        // 检查GC次数来判定是否内存过载
        long oldGcTimes = PhysicalUtils.getOldGcTimes();
        // 第一次检查,只给lastOldGcTimes赋初始值
        if (lastOldGcTimes == 0) {
            lastOldGcTimes = oldGcTimes;
            logger.debug("当前Old GC次数为{}", oldGcTimes);
            return;
        }
        logger.debug("当前Old GC次数为{}", oldGcTimes);
        if (oldGcTimes - lastOldGcTimes >= 1) {
            gcWarnCount++;
        } else {
            gcWarnCount = 0;
        }
        lastOldGcTimes = oldGcTimes;
        if (gcWarnCount >= 2) {
            logger.warn("已经连续{}次(每次间隔{}秒)发生了Old GC,需要降低压力",
                    OLD_GC_OVERLOAD_TIMES_THRESHOLD, MONITOR_INTERVAL);
            reducePressure();
            gcWarnCount = 0;
            return;
        }
    }

    private void reducePressure() {
        boolean result = WorkerProcessor.getProcessor().reducePressure();
        if (!result) {//如已经到了最低压力,则不再监控
            monitor = false;
        }
        try {
            Thread.sleep(waitTimeAfterReducePressure * 1000);//等待一段时间,以给降压后一段运行时间
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public int getCpuWarnCount() {
        return cpuWarnCount;
    }

    public int getGcWarnCount() {
        return gcWarnCount;
    }

    public boolean isMonitor() {
        return monitor;
    }

    public void setWaitTimeAfterReducePressure(int waitTimeAfterReducePressure) {
        this.waitTimeAfterReducePressure = waitTimeAfterReducePressure;
    }
}
