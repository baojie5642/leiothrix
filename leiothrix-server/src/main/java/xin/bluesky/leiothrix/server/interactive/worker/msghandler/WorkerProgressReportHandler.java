package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskProgress;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.storage.RangeStorage;

/**
 * @author 张轲
 *         worker.processor.threadnum.factor
 */
public class WorkerProgressReportHandler implements WorkerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProgressReportHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        PartitionTaskProgress progress = JSON.parseObject(message.getData(), PartitionTaskProgress.class);

        PartitionTask partitionTask = progress.getPartitionTask();
        String taskId = partitionTask.getTaskId();
        String tableName = partitionTask.getTableName();
        String rangeName = partitionTask.getPartitionRangeName();

        RangeStorage.setNameRangeStartIndex(taskId, tableName, rangeName, progress.getEndIndex() + 1);
        // 目的是刷新zookeeper节点的mtime,TimeoutPartitionTaskChecker会检查节点是否长期未被update,从而判定worker已经die,需要重新分配给其他worker
        RangeStorage.refreshRangeLastUpdateTime(taskId, tableName, rangeName);

        RangeStorage.updatePageExecutionStatistics(taskId, tableName, rangeName, progress.getStatistics());
        logger.debug("worker[ip={}]正在执行任务片[table={},rangeName={}],当前执行到{}",
                message.getIp(), tableName, rangeName, progress.getEndIndex());
    }
}
