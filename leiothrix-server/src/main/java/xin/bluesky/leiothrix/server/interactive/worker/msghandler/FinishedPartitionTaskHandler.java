package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;

import static xin.bluesky.leiothrix.server.bean.status.RangeStatus.FINISHED;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class FinishedPartitionTaskHandler implements WorkerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(FinishedPartitionTaskHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        PartitionTask partitionTask = JSON.parseObject(message.getData(), PartitionTask.class);

        String taskId = partitionTask.getTaskId();
        String rangeName = partitionTask.getPartitionRangeName();
        String tableName = partitionTask.getTableName();

        RangeStorage.setRangeStatus(taskId, tableName, rangeName, FINISHED);
        logger.info("worker完成了任务片[taskId={},tableName={},rangeName={}]", taskId, tableName, rangeName);
    }
}
