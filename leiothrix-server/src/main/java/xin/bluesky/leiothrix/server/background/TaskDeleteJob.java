package xin.bluesky.leiothrix.server.background;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.DateUtils2;
import xin.bluesky.leiothrix.server.interactive.client.TaskFileService;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author 张轲
 * @date 16/3/7
 */
public class TaskDeleteJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TaskDeleteJob.class);

    public static final long DELETE_DAY_THRESHOLD = 3;

    private static final long CHECK_INTERVAL = 60;

    private static final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("task-delete").build());

    public static void start() {
        logger.info("启动任务删除线程,定时删除完成时间距今超过{}天的任务,每隔{}秒检查一次,", DELETE_DAY_THRESHOLD, CHECK_INTERVAL);
        executorService.scheduleAtFixedRate(new TaskDeleteJob(), 0, CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        List<String> finishedTasks = TaskStorage.getAllFinishedTasks();
        finishedTasks.forEach(taskId -> {
            long finishedTime = TaskStorage.getTaskFinishedTime(taskId);
            if (new Date().getTime() - finishedTime > DELETE_DAY_THRESHOLD * 24 * 60 * 60 * 1000) {
                TaskStorage.delete(taskId);

                TaskFileService.deleteOnAllServers(taskId);

                logger.info("任务[taskId={}]于{}完成,离现在已经超过{}天,删除", taskId, DateUtils2.formatFull(finishedTime), DELETE_DAY_THRESHOLD);
            }
        });
    }
}
