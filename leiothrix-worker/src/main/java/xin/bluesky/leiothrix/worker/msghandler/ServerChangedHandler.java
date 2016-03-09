package xin.bluesky.leiothrix.worker.msghandler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.worker.ServerMessageHandler;
import xin.bluesky.leiothrix.worker.client.ServerChannel;

import java.util.Arrays;
import java.util.List;

/**
 * @author 张轲
 */
public class ServerChangedHandler implements ServerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServerChangedHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, String data) {
        if (StringUtils.isBlank(data)) {
            logger.warn("收到server发来的ServerChanged事件,但server列表为空,表明当前server都处于不可用状态");
            return;
        }

        logger.info("收到server发来的ServerChanged事件,新server列表为:{}", data);
        List<String> servers = Arrays.asList(data.split(","));
        ServerChannel.serverChanged(servers);
    }

}
