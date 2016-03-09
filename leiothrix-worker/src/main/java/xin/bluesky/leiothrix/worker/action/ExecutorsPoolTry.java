package xin.bluesky.leiothrix.worker.action;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class ExecutorsPoolTry {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorsPoolTry.class);

    private ThreadPoolExecutor executors;

    public ExecutorsPoolTry() {
        int executorNumber = calExecutorsNumbers();
        this.executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(executorNumber,
                new ThreadFactoryBuilder().setNameFormat("partition-task-runner-%d").build());

        logger.info("创建工作线程池,线程数量为:{}", executorNumber);
    }

    protected int calExecutorsNumbers() {
        return 3;
    }

    public void submit(Task taskExecutor) {
        executors.submit(taskExecutor);
    }

    public int getPoolSize() {
        return executors.getCorePoolSize();
    }

    public void awaitTermination() {
        try {

            while (executors.getActiveCount() != 0)
                Thread.sleep(5000);

            // 间隔一段时间后再检查一次,防止在线程池初始化时(得到任务之前)时被意外关闭
            Thread.sleep(3 * 1000);
            if (executors.getActiveCount() == 0) {
                logger.info("所有工作线程都确认结束");
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
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

    public static void main(String[] args) throws Exception {
        ExecutorsPoolTry poolTry = new ExecutorsPoolTry();
        poolTry.submit(new Task());
        for (int i = 0; i < 20; i++) {
            System.out.println(poolTry.executors.getActiveCount());
            Thread.sleep(1000);
        }

    }

    private static class Task implements Callable {
        @Override
        public Boolean call() throws Exception {
            Thread.sleep(3 * 1000);
            return true;
        }
    }
}
