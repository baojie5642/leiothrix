package xin.bluesky.leiothrix.worker.background;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.worker.WorkerProcessor;
import xin.bluesky.leiothrix.worker.action.ProcessorAnnouncer;

/**
 * 在worker进程关闭的时候,很可能由于各种原因导致不能被正常KILL(比如内存溢出),此时ShutdownHook做不了太多的事情,
 * 所以没有调用{@link WorkerProcessor#shutdown()}方法,只要尽量保证把这个关闭信息告诉server,就够了,
 * 其余在{@link WorkerProcessor#shutdown()}中做的进程内的清理动作,即使没能做成功,影响的也是进程内的事情,手工KILL -9就好了
 *
 * @author 张轲
 */
public class ShutdownHook extends Thread {

    private static Logger logger = LoggerFactory.getLogger(ShutdownHook.class);

    @Override
    public void run() {
        logger.info("执行ShutdownHook");
        ProcessorAnnouncer.decreaseWorkerNumber();
    }
}
