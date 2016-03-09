package xin.bluesky.leiothrix.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper;
import xin.bluesky.leiothrix.worker.ServerMessageHandler;
import xin.bluesky.leiothrix.worker.action.TaskContainer;

import static xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper.*;

/**
 * @author 张轲
 * worker.processor.threadnum.factor
 */
public class AcquireTaskResponseHandler implements ServerMessageHandler {

    private static final Logger logger= LoggerFactory.getLogger(AcquireTaskResponseHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, String data) {
        PartitionTaskWrapper wrapper = JSON.parseObject(data, PartitionTaskWrapper.class);
        logger.debug("接收到server返回的任务片详情:{}", wrapper.toString());

        switch (wrapper.getStatus()) {
            case STATUS_SUCCESS:
            case STATUS_WAIT_AND_TRY_LATER:
                TaskContainer.putPartitionTaskWrapper(wrapper);
                break;
            case STATUS_NO_TASK:
                // 忽略即可.Runner线程如果没有收到任务,在间隔一段时间后会自动结束
                break;
            default:
                break;
        }
    }
}
