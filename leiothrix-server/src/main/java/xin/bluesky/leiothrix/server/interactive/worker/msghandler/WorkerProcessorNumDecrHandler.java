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
public class WorkerProcessorNumDecrHandler implements WorkerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProcessorNumDecrHandler.class);

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
            int oldNumber = WorkerStorage.getWorkersNumber(workerIp);
            if (oldNumber > 0) {
                final int newNumber = oldNumber - 1;
                WorkerStorage.setWorkersNumber(workerIp, newNumber);
                logger.info("worker[ip={}]有一个worker进程结束,原worker数量是{},现在worker数量是{}", workerIp, oldNumber, newNumber);
            } else {
                logger.error("在decrease worker number时,其当前值为<=0,这是不应该发生的[workerIp={}]", workerIp);
            }
        }
    }
}
