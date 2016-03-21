package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.stat.ProcessorInfo;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageHandler;
import xin.bluesky.leiothrix.server.lock.ProcessorStartingHolder;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static xin.bluesky.leiothrix.model.stat.ProcessorInfo.STEP_EXIT;
import static xin.bluesky.leiothrix.model.stat.ProcessorInfo.STEP_STARTUP;

/**
 * @author 张轲
 */
public class ProcessorAnnounceHandler implements WorkerMessageHandler {

    public static final Logger logger = LoggerFactory.getLogger(ProcessorAnnounceHandler.class);

    private static Map<String, Object> locks = new ConcurrentHashMap();

    @Override
    public void handle(ChannelHandlerContext ctx, WorkerMessage message) {
        ProcessorInfo processor = JSON.parseObject(message.getData(), ProcessorInfo.class);
        String workerIp = message.getIp();

        synchronized (WorkerMessageHandler.class) {
            if (!locks.containsKey(workerIp)) {
                locks.put(workerIp, new Object());
            }
        }

        synchronized (locks.get(workerIp)) {
            String step = processor.getStep();
            switch (step) {
                case STEP_STARTUP:
                    startUpHandle(processor);
                    break;
                case STEP_EXIT:
                    exitHandle(processor);
                    break;
                default:
                    logger.error("错误的状态[传递进来的step是{}]", step);
                    break;
            }
        }
    }

    private void startUpHandle(ProcessorInfo processor) {
        final String workerIp = processor.getWorkerIp();
        int currentNumber = WorkerStorage.getWorkersNumber(workerIp);
        ProcessorStartingHolder holder = ProcessorStartingHolder.get(processor.getTaskId());

        if (processor.isSuccess()) {
            int newNumber = currentNumber + 1;
            WorkerStorage.setWorkersNumber(workerIp, newNumber);
            TaskStorage.addWorkerProcessor(processor.getTaskId(), workerIp, processor.getProcessorId(), processor.getTime());
            holder.setRunningSuccess(true);
            holder.unHold();
            logger.info("worker[ip={}]有一个worker进程启动,当前该worker上总共有{}个worker进程在运行", workerIp, newNumber);
        } else {
            holder.setRunningSuccess(false);
            holder.setErrorMsg(processor.getErrorMsg());
            holder.unHold();
        }
    }

    private void exitHandle(ProcessorInfo processor) {
        final String workerIp = processor.getWorkerIp();
        int currentNumber = WorkerStorage.getWorkersNumber(workerIp);

        if (processor.isSuccess()) {
            if (currentNumber <= 0) {
                logger.error("在扣减worker进程数量时,其当前值为<=0,这是不应该发生的[workerIp={}]", workerIp);
                return;
            }

            final int newNumber = currentNumber - 1;
            WorkerStorage.setWorkersNumber(workerIp, newNumber);
            TaskStorage.finishedWorkerProcessor(processor.getTaskId(), workerIp, processor.getProcessorId(), processor.getTime());

            if (StringUtils.isBlank(processor.getErrorMsg())) {
                logger.info("worker[ip={}]有一个worker进程成功结束,原worker数量是{},现在worker数量是{}", workerIp, currentNumber, newNumber);
            } else {
                logger.info("worker[ip={}]有一个worker进程成功结束但结束时抛出异常,原worker数量是{},现在worker数量是{},异常信息为:{}", workerIp, currentNumber, newNumber, processor.getErrorMsg());
            }
        } else {
            //todo: 退出失败怎么办?这意味这worker进程没有能够退出,此时需要系统通过kill -9 命令去关闭worker进程.目前还没能遇到这种场景.
        }
    }
}
