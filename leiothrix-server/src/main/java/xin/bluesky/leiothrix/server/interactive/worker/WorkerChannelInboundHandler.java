package xin.bluesky.leiothrix.server.interactive.worker;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.interactive.worker.msghandler.*;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.msg.WorkerMessageType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WorkerChannelInboundHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(WorkerChannelInboundHandler.class);

    private static Map<String, WorkerMessageHandler> map = new HashMap();

    private static Set<Channel> clientsSet = new HashSet();

    static {
        map.put(WorkerMessageType.ACQUIRE_TASK, new AcquireTaskHandler());
        map.put(WorkerMessageType.FINISHED_TASK, new FinishedPartitionTaskHandler());
        map.put(WorkerMessageType.EXECUTE_PROGRESS_REPORT, new WorkerProgressReportHandler());
        map.put(WorkerMessageType.PROCESSOR_ANNOUNCE, new ProcessorAnnounceHandler());
        map.put(WorkerMessageType.PING, new PingHandler());
    }

    public WorkerChannelInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        clientsSet.add(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        clientsSet.remove(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.debug("接收到worker发送来的数据:{}", msg);

        super.channelRead(ctx, msg);
        WorkerMessage message = JSON.parseObject((String) msg, WorkerMessage.class);

        WorkerMessageHandler handler = map.get(message.getType());
        if (handler == null) {
            logger.error("不支持该种类型[{}]的请求", message.getType());
            return;
        }

        handler.handle(ctx, message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error(ExceptionUtils.getStackTrace(cause));
        ctx.close();
    }

    public static Set<Channel> getClientsSet() {
        return clientsSet;
    }
}