package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import io.netty.channel.ChannelHandlerContext;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;

/**
 * @author 张轲
 */
public class PingHandler implements WorkerMessageHandler {

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        ctx.channel().writeAndFlush("pong\r\n");
    }
}
