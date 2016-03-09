package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.ServerMessage;
import xin.bluesky.leiothrix.model.msg.ServerMessageType;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper;
import xin.bluesky.leiothrix.server.action.PartitionAllocatorFactory;
import xin.bluesky.leiothrix.server.action.PartitionTaskFinder;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.action.exception.WaitAndTryLaterException;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.cache.PartitionTaskContainer;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.lock.LockFactory;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper.*;

/**
 * @author 张轲
 * worker.processor.threadnum.factor
 */
public class AcquireTaskHandler implements WorkerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(AcquireTaskHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        ServerMessage response = new ServerMessage();
        response.setType(ServerMessageType.ACQUIRE_TASK);

        String taskId = message.getData().toString();

        PartitionTaskContainer cache = PartitionTaskContainer.getInstance(taskId);
        PartitionTask partitionTask = cache.poll();

        if (partitionTask != null) {
            flushSuccessPartitionTask(ctx, response, taskId, partitionTask);
            return;
        }

        InterProcessMutex mutex = LockFactory.getPartitionTaskFindLock(taskId);
        boolean lockSuccess = false;

        try {
            lockSuccess = mutex.acquire(3, TimeUnit.SECONDS);
            if (lockSuccess && cache.isEmpty()) {
                PartitionTaskFinder finder = new PartitionTaskFinder(taskId, PartitionAllocatorFactory.get(taskId));
                List<PartitionTask> list = finder.find();
                cache.offer(list);
            }

            partitionTask = cache.poll();//这里一定能拿到的,如果没有任务,上面find的时候会抛出异常

            flushSuccessPartitionTask(ctx, response, taskId, partitionTask);
        } catch (Exception e) {
            flushNotSuccessPartitionTask(ctx, message, response, taskId, e);
        } finally {
            if (lockSuccess) {
                try {
                    mutex.release();
                } catch (Exception ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        }
    }

    private void flushNotSuccessPartitionTask(ChannelHandlerContext ctx, WorkerMessage message, ServerMessage response, String taskId, Exception e) {
        PartitionTaskWrapper wrapper = handleException(taskId, e, response, message.getIp());
        response.setData(JSON.toJSONString(wrapper));
        ctx.writeAndFlush(JSON.toJSONString(response) + "\r\n");
    }

    private void flushSuccessPartitionTask(ChannelHandlerContext ctx, ServerMessage response, String taskId, PartitionTask partitionTask) {
        RangeStorage.setRangeStatus(taskId,
                partitionTask.getTableName(), partitionTask.getPartitionRangeName(), RangeStatus.PROCESSING);
        response.setData(JSON.toJSONString(new PartitionTaskWrapper(STATUS_SUCCESS, partitionTask)));
        ctx.writeAndFlush(JSON.toJSONString(response) + "\r\n");
    }

    private PartitionTaskWrapper handleException(String taskId, Exception e, ServerMessage response, String workerIp) {
        //设置该任务的当前资源已经足够了(因为其已拥有的资源也已经拿不到新任务片),以使得补偿线程不再对该任务分配新资源
        if (e instanceof NoTaskException) {
            TaskStorage.setResourceEnough(taskId, true);
            //todo: 怎么根据queue的消费情况,来置resourceEnough为false呢
            logger.info("worker[ip={}]请求新任务片,但当前没有可供其执行的任务.", workerIp);
            return new PartitionTaskWrapper(STATUS_NO_TASK, null);
        } else if (e instanceof WaitAndTryLaterException) {
            TaskStorage.setResourceEnough(taskId, true);
            logger.info("worker[ip={}]请求新任务片,但当前需要其等待并稍后重试.", workerIp);
            return new PartitionTaskWrapper(STATUS_WAIT_AND_TRY_LATER, null);
        } else {
            logger.error(e.getMessage(), e);
            return new PartitionTaskWrapper(STATUS_FAIL, null);
        }
    }
}
