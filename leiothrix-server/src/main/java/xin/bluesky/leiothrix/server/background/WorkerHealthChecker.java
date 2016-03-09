package xin.bluesky.leiothrix.server.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static xin.bluesky.leiothrix.common.net.NetUtils.pingSuccess;

/**
 * @author 张轲
 */
public class WorkerHealthChecker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerHealthChecker.class);

    private static ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("worker-status-checker").build());

    private static final long CHECK_INTERVAL = 5;

    public static void start() {
        logger.info("启动worker健康检查线程,每隔{}秒检查一次", CHECK_INTERVAL);
        checker.scheduleAtFixedRate(new WorkerHealthChecker(), 0, CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    private WorkerHealthChecker() {

    }

    @Override
    public void run() {
        List<String> workerList = WorkerStorage.getAllWorkers();

        for (String worker : workerList) {
            if (pingSuccess(worker)) {
                continue;
            }

            logger.error("server无法连通worker[ip={}],将该worker从zookeeper中移除.", worker);

            // 移除该worker
            WorkerStorage.delete(worker);
        }
    }


}
