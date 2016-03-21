package xin.bluesky.leiothrix.worker.executor;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.stat.ProcessorInfo;
import xin.bluesky.leiothrix.worker.client.ServerChannel;
import xin.bluesky.leiothrix.worker.conf.Settings;

import java.lang.management.ManagementFactory;

import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.PROCESSOR_ANNOUNCE;

/**
 * 进程广播者.当进程启动的时候,需要通知server进行增加计数;当进程关闭的时候,同样也要通知server进行扣减计数.
 *
 * <p>该类中的方法都是线程安全,
 * 同时具备幂等性的(增加或减少的消息不论调用方法多少次,都只会向server发送一次消息),所以可以放心调用</p>
 *
 * @author 张轲
 */
public class ProcessorAnnouncer {

    public static final Logger logger = LoggerFactory.getLogger(ProcessorAnnouncer.class);

    private static int counter = 0;

    public synchronized static void announceStartupSuccess() {
        startup(true, null);
    }

    public synchronized static void announceStartupFail(String errorMsg) {
        startup(false, errorMsg);
    }

    private synchronized static void startup(boolean success, String errorMsg) {
        if (counter == 0) {
            ProcessorInfo processInfo = getStaticProcessInfo();
            processInfo.setStep(ProcessorInfo.STEP_STARTUP);
            processInfo.setSuccess(success);
            processInfo.setErrorMsg(errorMsg);

            WorkerMessage message = new WorkerMessage(PROCESSOR_ANNOUNCE, JSON.toJSONString(processInfo), Settings.getWorkerIp());
            ServerChannel.send(message);

            logger.info("向server宣告进程启动");
            counter++;
        }
    }

    public synchronized static void announceExit() {
        announceExit(null);
    }

    public synchronized static void announceExit(String errorMsg) {
        if (counter == 1) {
            ProcessorInfo processInfo = getStaticProcessInfo();
            processInfo.setStep(ProcessorInfo.STEP_EXIT);
            processInfo.setSuccess(true);
            processInfo.setErrorMsg(errorMsg);

            WorkerMessage message = new WorkerMessage(PROCESSOR_ANNOUNCE, JSON.toJSONString(processInfo), Settings.getWorkerIp());
            ServerChannel.send(message);

            logger.info("向server宣告进程结束");
            counter--;

            try {
                Thread.sleep(1000);//等待2秒确保信息发送出去了
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    protected static ProcessorInfo getStaticProcessInfo() {
        String processorId = ManagementFactory.getRuntimeMXBean().getName();
        if (processorId.indexOf("@") > 0) {
            processorId = processorId.substring(0, processorId.indexOf("@"));
        }
        ProcessorInfo info = new ProcessorInfo(Settings.getTaskId(), Settings.getWorkerIp(), processorId);
        return info;
    }
}
