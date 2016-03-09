package xin.bluesky.leiothrix.worker;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author 张轲
 * worker.processor.threadnum.factor
 */
public interface ServerMessageHandler {

    void handle(ChannelHandlerContext ctx, String data);
}
