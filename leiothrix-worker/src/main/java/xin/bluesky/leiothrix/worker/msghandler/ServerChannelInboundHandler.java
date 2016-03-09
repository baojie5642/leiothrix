package xin.bluesky.leiothrix.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.ServerMessage;
import xin.bluesky.leiothrix.model.msg.ServerMessageType;
import xin.bluesky.leiothrix.worker.ServerMessageHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 张轲
 * @date 16/1/23
 */
public class ServerChannelInboundHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ServerChannelInboundHandler.class);

    private static final Map<String, ServerMessageHandler> map = new HashMap();

    static {
        map.put(ServerMessageType.ACQUIRE_TASK, new AcquireTaskResponseHandler());
        map.put(ServerMessageType.SERVER_UPDATED, new ServerChangedHandler());
        map.put(ServerMessageType.CANCEL_TASK,new TaskCancelHandler());
    }

    public ServerChannelInboundHandler() {
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        super.channelRead(ctx, msg);
        ServerMessage serverMessage = JSON.parseObject((String) msg, ServerMessage.class);

        ServerMessageHandler handler = map.get(serverMessage.getType());
        if (handler == null) {
            // do nothing
            return;
        }

        handler.handle(ctx, serverMessage.getData());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error(ExceptionUtils.getStackTrace(cause));
        ctx.close();
    }
}
