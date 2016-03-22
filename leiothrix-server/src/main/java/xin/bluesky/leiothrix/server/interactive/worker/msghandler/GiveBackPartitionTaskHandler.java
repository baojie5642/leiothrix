package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.cache.PartitionTaskContainer;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.storage.RangeStorage;

/**
 * @author 张轲
 */
public class GiveBackPartitionTaskHandler implements WorkerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(GiveBackPartitionTaskHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        PartitionTask partitionTask = JSON.parseObject(message.getData(), PartitionTask.class);

        String taskId = partitionTask.getTaskId();
        PartitionTask stored = RangeStorage.getPartitionTask(partitionTask.getTaskId(), partitionTask.getTableName(), partitionTask.getRangeName());
        PartitionTaskContainer.getInstance(taskId).offer(stored);

        logger.info("任务片[tableName={},rangeName={}被成功加入待分配队列,其startIndex={},endIndex={}",
                stored.getTableName(), stored.getRangeName(), stored.getRowStartIndex(), stored.getRowEndIndex());
    }
}
