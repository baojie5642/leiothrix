package xin.bluesky.leiothrix.worker.action;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.stat.TaskWorkerProcessorInfo;
import xin.bluesky.leiothrix.worker.Settings;
import xin.bluesky.leiothrix.worker.client.ServerChannel;

import java.lang.management.ManagementFactory;

import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.WORKER_NUM_DECR;
import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.WORKER_NUM_INCR;

/**
 * 进程广播者.当进程启动的时候,需要通知server进行增加计数;当进程关闭的时候,同样也要通知server进行扣减计数.
 *
 * <p>{@link #increaseProcessorNumber()}和{@link #decreaseProcessorNumber()}都是线程安全,
 * 同时具备幂等性的(增加或减少的消息不论调用方法多少次,都只会向server发送一次消息),所以可以放心调用</p>
 *
 * @author 张轲
 */
public class ProcessorAnnouncer {

    public static final Logger logger = LoggerFactory.getLogger(ProcessorAnnouncer.class);

    private static int counter = 0;

    public synchronized static void decreaseProcessorNumber() {
        if (counter == 1) {
            WorkerMessage message = new WorkerMessage(WORKER_NUM_DECR, JSON.toJSONString(getProcessInfo()), Settings.getWorkerIp());
            ServerChannel.send(message);
            logger.info("向server宣告进程结束");
            counter--;
            try {
                Thread.sleep(2000);//等待2秒确保信息发送出去了
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public synchronized static void increaseProcessorNumber() {
        if (counter == 0) {
            WorkerMessage message = new WorkerMessage(WORKER_NUM_INCR, JSON.toJSONString(getProcessInfo()), Settings.getWorkerIp());
            ServerChannel.send(message);
            logger.info("向server宣告进程启动");
            counter++;
        }
    }

    protected static TaskWorkerProcessorInfo getProcessInfo() {
        String processorId = ManagementFactory.getRuntimeMXBean().getName();
        if (processorId.indexOf("@") > 0) {
            processorId = processorId.substring(0, processorId.indexOf("@"));
        }
        TaskWorkerProcessorInfo info = new TaskWorkerProcessorInfo(Settings.getTaskId(), Settings.getWorkerIp(), processorId);
        return info;
    }
}
