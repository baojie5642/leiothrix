package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 张轲
 * @date 16/2/16
 */
public class WorkerProcessorNumIncrHandler implements WorkerMessageHandler {

    public static final Logger logger = LoggerFactory.getLogger(WorkerProcessorNumIncrHandler.class);

    private static Map<String, Object> locks = new ConcurrentHashMap();

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        String workerIp = message.getIp();

        synchronized (WorkerMessageHandler.class) {
            if (!locks.containsKey(workerIp)) {
                locks.put(workerIp, new Object());
            }
        }

        synchronized (locks.get(workerIp)) {
            int currentNumber = WorkerStorage.getWorkersNumber(workerIp);
            final int newNumber = currentNumber + 1;
            WorkerStorage.setWorkersNumber(workerIp, newNumber);
            logger.info("worker[ip={}]有一个worker进程启动,当前该worker上总共有{}个worker进程在运行", workerIp, newNumber);
        }
    }
}
