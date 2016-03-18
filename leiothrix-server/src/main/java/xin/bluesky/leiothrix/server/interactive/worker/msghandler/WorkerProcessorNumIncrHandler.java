package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.stat.TaskWorkerProcessorInfo;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 张轲
 */
public class WorkerProcessorNumIncrHandler implements WorkerMessageHandler {

    public static final Logger logger = LoggerFactory.getLogger(WorkerProcessorNumIncrHandler.class);

    private static Map<String, Object> locks = new ConcurrentHashMap();

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        TaskWorkerProcessorInfo info = JSON.parseObject(message.getData(), TaskWorkerProcessorInfo.class);
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
            TaskStorage.addWorkerProcessor(info.getTaskId(), workerIp, info.getProcessorId(), info.getTime());
            logger.info("worker[ip={}]有一个worker进程启动,当前该worker上总共有{}个worker进程在运行", workerIp, newNumber);
        }
    }
}
