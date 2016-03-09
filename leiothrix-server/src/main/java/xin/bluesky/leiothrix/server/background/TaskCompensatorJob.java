package xin.bluesky.leiothrix.server.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.background.compensate.CompensateSingleTaskStrategy;
import xin.bluesky.leiothrix.server.background.compensate.CompensateStrategy;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static xin.bluesky.leiothrix.server.Constant.SYS_PROP_WORKER_COMPENSATE_ENABLE;
import static xin.bluesky.leiothrix.server.Constant.SYS_PROP_WORKER_COMPENSATE_ENABLE_VALUE_FALSE;

/**
 * 当worker进程执行完毕后(或worker进程crash),资源会被释放出来,此时应该针对正在运行中的任务来进行补偿,给它们分配更多的worker进程.
 *
 * <p>分配资源有多种算法,目前包括:
 * <ul>
 * <li>
 * 1. 最老任务优先 {@link CompensateSingleTaskStrategy}
 * </li>
 * </ul>
 * </p>
 *
 * @author 张轲
 * @date 16/2/16
 */
public class TaskCompensatorJob implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(TaskCompensatorJob.class);

    private static final int CHECK_INTERVAL = 60;

    private static ScheduledExecutorService executorService;

    private CompensateStrategy compensateStrategy;

    private TaskCompensatorJob(CompensateStrategy compensateStrategy) {
        this.compensateStrategy = compensateStrategy;
    }

    //todo:当前jar文件是存储到磁盘上,每台server都会存.考虑支持更多的方式,如OSS
    public static void start() {
        if (SYS_PROP_WORKER_COMPENSATE_ENABLE_VALUE_FALSE.equals(ServerConfigure.get(SYS_PROP_WORKER_COMPENSATE_ENABLE, false))) {
            logger.info("根据配置,不需要启动任务补偿线程");
            return;
        }

        logger.info("启动任务补偿线程,每隔{}秒执行一次", CHECK_INTERVAL);
        executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("task-compensator").build());

        TaskCompensatorJob boost = new TaskCompensatorJob(new CompensateSingleTaskStrategy());
        executorService.scheduleWithFixedDelay(boost, 10, CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        compensateStrategy.execute();
    }
}

