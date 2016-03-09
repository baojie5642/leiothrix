package xin.bluesky.leiothrix.worker.msghandler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.worker.ServerMessageHandler;
import xin.bluesky.leiothrix.worker.Settings;
import xin.bluesky.leiothrix.worker.WorkerProcessor;

/**
 * @author 张轲
 */
public class TaskCancelHandler implements ServerMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(TaskCancelHandler.class);

    @Override
    public void handle(ChannelHandlerContext ctx, String data) {
        String taskId = data;

        if (notMyTask(taskId)) {
            return;
        }

        synchronized (this) {
            final WorkerProcessor processor = WorkerProcessor.getProcessor();
            if (processor.isRunning()) {//避免多线程情况下多次触发关闭操作
                logger.info("任务被取消,当前worker进程被要求关闭");
                new Thread(() -> {//启用另一个线程来执行,以避免在netty的handle方法体内,调用shutdown来关闭netty客户端,造成意料外的行为
                    try {
                        processor.shutdown();
                    } catch (Exception e) {
                        logger.error("关闭worker进程时发生异常,异常信息:{}", ExceptionUtils.getStackTrace(e));
                        logger.info("开始使用System.exit(1)来强制退出");
                        System.exit(1);
                    }
                }).start();
            }
            try {
                Thread.sleep(300);//等一段时间,以保证上面创建的进程关闭线程被启动(潜在意义是状态已不再是isRunning了)
            } catch (InterruptedException e) {
                logger.error("等待被中断");
            }
        }
    }

    private boolean notMyTask(String taskId) {
        return !Settings.getTaskId().equals(taskId);
    }

}
