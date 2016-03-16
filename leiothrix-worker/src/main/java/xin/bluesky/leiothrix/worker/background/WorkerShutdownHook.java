package xin.bluesky.leiothrix.worker.background;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.worker.Settings;
import xin.bluesky.leiothrix.worker.client.ServerChannel;

import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.WORKER_NUM_DECR;

/**
 * @author 张轲
 */
public class WorkerShutdownHook extends Thread {

    private static Logger logger = LoggerFactory.getLogger(WorkerShutdownHook.class);

    @Override
    public void run() {
        WorkerMessage message = new WorkerMessage(WORKER_NUM_DECR, null, Settings.getWorkerIp());
        ServerChannel.send(message);
        try {
            Thread.sleep(2000);//等待2秒确保信息发送出去了
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
