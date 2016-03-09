package xin.bluesky.leiothrix.worker;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author 张轲
 * @date 16/1/24
 */
public interface ServerMessageHandler {

    void handle(ChannelHandlerContext ctx, String data);
}
