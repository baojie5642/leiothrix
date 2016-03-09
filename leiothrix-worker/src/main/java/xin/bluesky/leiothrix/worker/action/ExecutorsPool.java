package xin.bluesky.leiothrix.worker.action;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.worker.Settings;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static java.math.BigDecimal.ROUND_HALF_DOWN;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class ExecutorsPool {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorsPool.class);

    private ThreadPoolExecutor executors;

    public ExecutorsPool() {
        int executorNumber = calExecutorsNumbers();
        this.executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(executorNumber,
                new ThreadFactoryBuilder().setNameFormat("partition-task-runner-%d").build());

        logger.info("创建工作线程池,线程数量为:{}", executorNumber);
    }

    protected int calExecutorsNumbers() {
        // 得到物理机的cpu processor数和可用内存
        int cpuNumbers = Runtime.getRuntime().availableProcessors();
        long freeMemory = Runtime.getRuntime().freeMemory();

        // 用物理机的可用内存除以分配给单个worker进程的内存,以得到最大worker进程数
        int workerHeapSize = (int) ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() >> 20;//以M为单位
        int maxWorkerProcessNum = (int) (freeMemory >> 20) / workerHeapSize;
        if (maxWorkerProcessNum == 0) {
            maxWorkerProcessNum = 1;
        }

        // 用cpu processor数除以最大worker进程数,得到大致每个worker进程占的cpu.保留一位精度并舍弃多余位数
        BigDecimal roughCpuNumbersForMe = new BigDecimal(cpuNumbers / maxWorkerProcessNum).setScale(1, ROUND_HALF_DOWN);

        // 每个cpu processor对应的线程数,向下取整
        return roughCpuNumbersForMe.multiply(new BigDecimal(Settings.getThreadNumFactor())).intValue();
    }

    public void submit(TaskExecutor taskExecutor) {
        executors.submit(taskExecutor);
    }

    public int getPoolSize() {
        return executors.getCorePoolSize();
    }

    public void shutdown() {
        executors.shutdown();
        waitTerminated();
        logger.info("成功关闭工作线程池");
    }

    private void waitTerminated() {
        while (!executors.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("工作线程池线程在关闭的时候被中断");
            }
        }
    }
}
