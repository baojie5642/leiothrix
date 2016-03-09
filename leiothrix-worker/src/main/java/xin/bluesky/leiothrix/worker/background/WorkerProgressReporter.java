package xin.bluesky.leiothrix.worker.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.net.NetUtils;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskProgress;
import xin.bluesky.leiothrix.worker.WorkerProcessor;
import xin.bluesky.leiothrix.worker.client.ServerChannel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static com.alibaba.fastjson.JSON.toJSONString;
import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.WORKER_PROGRESS_REPORT;

/**
 * @author 张轲
 * @date 16/1/26
 */
public class WorkerProgressReporter implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProgressReporter.class);

    private static ExecutorService reporter = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("worker-status-report").build());

    private static BlockingQueue<PartitionTaskProgress> processingPartitionTaskQueue = new LinkedBlockingDeque<>();

    public void start() {
        logger.info("启动向server上报worker执行进度的线程");
        reporter.submit(this);
    }

    public void reportProgress(PartitionTask partitionTask, long endIndex) {
        processingPartitionTaskQueue.offer(new PartitionTaskProgress(partitionTask, endIndex));
    }

    public WorkerProgressReporter() {

    }

    @Override
    public void run() {
        while (WorkerProcessor.getProcessor().isRunning()) {
            try {
                PartitionTaskProgress progress = processingPartitionTaskQueue.take();
                final String localIp = NetUtils.getLocalIp();
                WorkerMessage message = new WorkerMessage(WORKER_PROGRESS_REPORT, toJSONString(progress), localIp);
                ServerChannel.send(message);
                logger.debug("worker:{}执行任务片[taskId={},tableName={},rangeName={}],当前执行到{}",
                        localIp, progress.getPartitionTask().getTaskId(), progress.getPartitionTask().getTableName(), progress.getPartitionTask().getPartitionRangeName(), progress.getEndIndex());
            } catch (InterruptedException e) {
            }
        }
    }

    public void shutdown() {
        reporter.shutdownNow();
        waitTerminated();
        logger.info("成功关闭向server上报worker执行进度的线程");
    }

    private void waitTerminated() {
        while (!reporter.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("向server上报worker执行进度的线程在关闭的时候被中断");
            }
        }
    }
}
