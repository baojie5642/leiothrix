package xin.bluesky.leiothrix.worker;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;
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

        init();
    }

    private void init() {
        SettingInit.init(configuration);

        this.executorsPool = new ExecutorsPool();

        this.countDownLatch = new CountDownLatch(executorsPool.getPoolSize());

        this.progressReporter = new WorkerProgressReporter();

        this.status = NOT_STARTED;
    }

    public void start() {

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

            ServerChannel.shutdown();

            Thread.sleep(3 * 1000);

            logger.info("worker进程成功退出");

            ProcessorAnnouncer.announceExit();
        } catch (Throwable e) {
            String errorMsg = StringEscapeUtils.escapeJava(ExceptionUtils.getStackTrace(e));
            ProcessorAnnouncer.announceExit(errorMsg);
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
