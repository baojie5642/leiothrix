package xin.bluesky.leiothrix.worker;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;
import xin.bluesky.leiothrix.worker.background.ResourceMonitor;
import xin.bluesky.leiothrix.worker.background.ShutdownHook;
import xin.bluesky.leiothrix.worker.client.ServerChannel;
import xin.bluesky.leiothrix.worker.conf.SettingInit;
import xin.bluesky.leiothrix.worker.conf.Settings;
import xin.bluesky.leiothrix.worker.conf.WorkerConfiguration;
import xin.bluesky.leiothrix.worker.executor.ExecutorsPool;
import xin.bluesky.leiothrix.worker.executor.ProcessorAnnouncer;
import xin.bluesky.leiothrix.worker.executor.TaskExecutor;
import xin.bluesky.leiothrix.worker.report.WorkerProgressReporter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static xin.bluesky.leiothrix.worker.WorkerProcessor.Status.*;

/**
 * @author 张轲
 */
public class WorkerProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProcessor.class);

    private static WorkerProcessor processor;

    private ExecutorsPool executorsPool;

    private WorkerProgressReporter progressReporter;

    private ResourceMonitor resourceMonitor;

    private volatile Status status;

    private ReentrantLock lock = new ReentrantLock();

    private CountDownLatch countDownLatch;

    private WorkerConfiguration configuration;

    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("线程[id={},name={}]出现异常:{}", t.getId(), t.getName(), ExceptionUtils.getStackTrace(e));
        });
    }

    public WorkerProcessor(WorkerConfiguration configuration) {

        synchronized (WorkerProcessor.class) {
            if (processor != null) {
                throw new RuntimeException("已经创建了一个worker进程,不能重复创建");
            }
            processor = this;
        }

        this.configuration = configuration;
        this.progressReporter = new WorkerProgressReporter();
        this.resourceMonitor = new ResourceMonitor();
        this.status = NOT_STARTED;
    }

    private void beforeStart() {
        SettingInit.init(configuration);

        // 线程池的数量要依赖于server传递过来的参数,所以必须在SettingInit.init之后才能调用
        this.executorsPool = new ExecutorsPool();
        this.countDownLatch = new CountDownLatch(executorsPool.getPoolSize());
    }

    public void start() {
        beforeStart();

        try {
            lock.lock();
            if (status != NOT_STARTED) {
                lock.unlock();
                throw new Exception("worker进程已经启动/或已关闭,不能再次启动");
            }

            logger.info("worker进程开始启动");
            status = RUNNING;
            lock.unlock();

            ServerChannel.connect(Settings.getServersIp(), Settings.getServerPort());

            Runtime.getRuntime().addShutdownHook(new ShutdownHook());

            submitExecutor();

            progressReporter.start();

            resourceMonitor.start();

            ProcessorAnnouncer.announceStartupSuccess();

            awaitTermination();
        } catch (Throwable e) {
            logger.error("worker启动过程中出现异常:{}", getStackTrace(e));
            ProcessorAnnouncer.announceStartupFail(StringEscapeUtils.escapeJava(ExceptionUtils.getStackTrace(e)));
        } finally {
            shutdown();
        }
    }

    private void submitExecutor() {
        for (int i = 0; i < executorsPool.getPoolSize(); i++) {
            executorsPool.submit(new TaskExecutor(progressReporter, countDownLatch));
        }
    }

    public void awaitTermination() throws InterruptedException {
        countDownLatch.await();
        logger.info("所有工作线程都已结束");
    }

    public boolean isRunning() {
        return status == RUNNING;
    }

    public boolean reducePressure() {
        int size = executorsPool.getRemainingExecutorSize();
        if (size == 1) {
            logger.info("当前只有1个工作线程在执行,不再降压");
            return false;
        }
        int reduceSize = size / 5 == 0 ? 1 : size / 5;
        executorsPool.rescheduleExecutor(reduceSize);
        logger.info("本次停止{}个工作线程以降低压力,降压后有{}个工作线程", reduceSize, size - reduceSize);
        return true;
    }

    public void shutdown() {
        lock.lock();
        if (!isRunning()) {
            lock.unlock();
            return;
        }
        status = SHUTDOWN;
        lock.unlock();

        logger.info("开始退出worker进程");

        status = SHUTDOWN;

        try {
            executorsPool.shutdown();

            progressReporter.shutdown();

            JdbcTemplate.destroy();

            Thread.sleep(3 * 1000);

            logger.info("worker进程成功退出");

            ProcessorAnnouncer.announceExit();

        } catch (Throwable e) {
            String errorMsg = StringEscapeUtils.escapeJava(ExceptionUtils.getStackTrace(e));
            ProcessorAnnouncer.announceExit(errorMsg);
        } finally {
            try {
                ServerChannel.shutdown();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public Status getStatus() {
        return status;
    }

    public enum Status {
        NOT_STARTED, RUNNING, SHUTDOWN
    }

    public static WorkerProcessor getProcessor() {
        return processor;
    }

}
