package xin.bluesky.leiothrix.server.background;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.Server;

/**
 * @author 张轲
 * @date 16/2/15
 */
public class ShutDownHook extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ShutDownHook.class);

    @Override
    public void run() {
        logger.info("执行ShutDownHook");

        Server.getServer().releaseLeaderSelector();
    }
}
