package xin.bluesky.leiothrix.server.interactive.worker;

import io.netty.channel.ChannelHandlerContext;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;

/**
 * @author 张轲
 * @date 16/1/24
 */
public interface WorkerMessageHandler {

    void handle(ChannelHandlerContext ctx, WorkerMessage message);
}
