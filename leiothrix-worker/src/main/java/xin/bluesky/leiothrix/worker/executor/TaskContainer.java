package xin.bluesky.leiothrix.worker.executor;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.msg.WorkerMessageType;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper;
import xin.bluesky.leiothrix.worker.conf.Settings;
import xin.bluesky.leiothrix.worker.client.ServerChannel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author 张轲
 */
public class TaskContainer {

    private static final Logger logger = LoggerFactory.getLogger(TaskContainer.class);

    private static TaskContainer instance = new TaskContainer();

    private BlockingQueue<PartitionTaskWrapper> partitionTaskBlockingQueue = new LinkedBlockingQueue<>();

    private TaskContainer() {

    }

    public static PartitionTaskWrapper takePartitionTaskWrapper(long timeout, TimeUnit timeUnit) throws InterruptedException {
        //这里等待timeout时间,理论上不应该有等不到任务的风险(在timeout时间范围内服务端未返回)
        ServerChannel.send(new WorkerMessage(WorkerMessageType.ACQUIRE_TASK, Settings.getTaskId(), Settings.getWorkerIp()));
        return instance.partitionTaskBlockingQueue.poll(timeout, timeUnit);
    }

    public static void putPartitionTaskWrapper(PartitionTaskWrapper partitionTaskWrapper) {
        try {
            instance.partitionTaskBlockingQueue.put(partitionTaskWrapper);
        } catch (InterruptedException e) {
            logger.error("将任务放入队列时被Interrupted!", ExceptionUtils.getStackTrace(e));
        }
    }
}
