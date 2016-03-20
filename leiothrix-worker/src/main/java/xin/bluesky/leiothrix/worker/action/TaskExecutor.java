package xin.bluesky.leiothrix.worker.action;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.msg.WorkerMessageType;
import xin.bluesky.leiothrix.model.task.partition.ExecutionStatistics;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskProgress;
import xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper;
import xin.bluesky.leiothrix.worker.Settings;
import xin.bluesky.leiothrix.worker.WorkerProcessor;
import xin.bluesky.leiothrix.worker.api.DatabasePageDataHandler;
import xin.bluesky.leiothrix.worker.background.WorkerProgressReporter;
import xin.bluesky.leiothrix.worker.client.ServerChannel;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper.STATUS_SUCCESS;
import static xin.bluesky.leiothrix.model.task.partition.PartitionTaskWrapper.STATUS_WAIT_AND_TRY_LATER;

/**
 * @author 张轲
 */
public class TaskExecutor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private static final int ACQUIRE_TASK_TIMEOUT = 15;

    private WorkerProgressReporter progressReporter;

    private CountDownLatch countDownLatch;

    public TaskExecutor(WorkerProgressReporter progressReporter, CountDownLatch countDownLatch) {
        this.progressReporter = progressReporter;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            while (ableRunning()) {
                PartitionTaskWrapper wrapper = TaskContainer.takePartitionTaskWrapper(ACQUIRE_TASK_TIMEOUT, SECONDS);

                if (wrapper == null) {
                    logger.info("工作线程{}在{}秒内没有获得新任务,结束", Thread.currentThread().getName(), ACQUIRE_TASK_TIMEOUT);
                    return;
                }

                switch (wrapper.getStatus()) {
                    case STATUS_WAIT_AND_TRY_LATER:
                        tryLater();
                        break;
                    case STATUS_SUCCESS:
                        execute(wrapper);
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("执行任务片时出错,异常:{}", ExceptionUtils.getStackTrace(e));
        } finally {
            countDownLatch.countDown();
        }

        return;
    }

    private boolean ableRunning() {
        return WorkerProcessor.getProcessor().isRunning();
    }

    private void tryLater() throws InterruptedException {
        Thread.sleep(10 * 1000);
    }

    private void execute(PartitionTaskWrapper wrapper) {
        StopWatch watch = new StopWatch();
        watch.start();
        PartitionTask partitionTask = wrapper.getPartitionTask();
        logger.info("得到新的任务片:{}", partitionTask);

        JdbcTemplate jdbcTemplate = new JdbcTemplate(partitionTask.getDatabaseInfo());

        execute(partitionTask, jdbcTemplate);
        watch.stop();

        notifyServerFinished(partitionTask);

        logger.info("本次任务片[table={},startIndex={},endIndex={}]执行结束,总共耗时{}毫秒",
                partitionTask.getTableName(), partitionTask.getRowStartIndex(),
                partitionTask.getRowEndIndex(), watch.getTime());
    }

    private void execute(PartitionTask partitionTask, JdbcTemplate jdbcTemplate) {
        long startIndex = partitionTask.getRowStartIndex();
        while (ableRunning()) {
            long endIndex = startIndex + Settings.getRangePageSize() - 1;
            if (endIndex > partitionTask.getRowEndIndex()) {
                ExecutionStatistics statistics = executePage(partitionTask, jdbcTemplate, startIndex, partitionTask.getRowEndIndex());
                progressReporter.reportProgress(new PartitionTaskProgress(partitionTask, partitionTask.getRowEndIndex(), statistics));
                break;
            } else {
                ExecutionStatistics statistics = executePage(partitionTask, jdbcTemplate, startIndex, endIndex);
                progressReporter.reportProgress(new PartitionTaskProgress(partitionTask, partitionTask.getRowEndIndex(), statistics));
                startIndex = endIndex + 1;
            }
        }
    }

    private ExecutionStatistics executePage(PartitionTask partitionTask, JdbcTemplate jdbcTemplate, long startIndex, long endIndex) {
        ExecutionStatistics statistics = new ExecutionStatistics();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String columns = StringUtils.isBlank(partitionTask.getColumnNames()) ? "*" : partitionTask.getColumnNames();
        String sql = StringUtils2.append("select ", columns, " from ", partitionTask.getTableName(),
                " where ", partitionTask.getPrimaryKey(), " >= ?",
                " and ", partitionTask.getPrimaryKey(), " <= ?");
        if (StringUtils.isNotBlank(partitionTask.getWhere())) {
            sql = StringUtils2.append(sql, " and (" + partitionTask.getWhere(), ")");
        }

        List<JSONObject> result = jdbcTemplate.query(sql, startIndex, endIndex);
        statistics.setHandledRecordNum(result.size());
        stopWatch.stop();
        long queryUsingTime = stopWatch.getTime();

        stopWatch.reset();
        stopWatch.start();
        DatabasePageDataHandler databasePageDataHandler = Settings.getConfiguration().getDatabasePageDataHandler();
        try {
            databasePageDataHandler.handle(partitionTask.getTableName(), partitionTask.getPrimaryKey(), result);
            statistics.setSuccessRecordNum(result.size());
        } catch (Throwable e) {
            databasePageDataHandler.exceptionCaught(partitionTask.getTableName(), result, new Exception(e));
            statistics.setFailRecordNum(result.size());
            statistics.setFailPageName(startIndex + "-" + endIndex);
            statistics.setExceptionMsg(e.getMessage());
        }
        stopWatch.stop();

        long handleUsingTime = stopWatch.getTime();
        long totalTime = queryUsingTime + handleUsingTime;

        statistics.setQueryUsingTime(queryUsingTime);
        statistics.setHandleUsingTime(handleUsingTime);
        statistics.setTotalTime(totalTime);

        logger.info("本次任务片分页查询[table={},startIndex={},endIndex={}]查询结束,有{}行数据,查询耗时{}毫秒,处理耗时{}毫秒,总共耗时{}毫秒",
                partitionTask.getTableName(), partitionTask.getRowStartIndex(),
                partitionTask.getRowEndIndex(), result.size(), queryUsingTime, handleUsingTime, totalTime);
        return statistics;
    }

    private int calQueryPage(PartitionTask partitionTask) {
        int page = (int) ((partitionTask.getRowEndIndex() - partitionTask.getRowStartIndex()) / Settings.getRangePageSize());
        long balance = (partitionTask.getRowEndIndex() - partitionTask.getRowStartIndex()) % Settings.getRangePageSize();
        if (balance != 0) {
            page = page + 1;
        }
        return page;
    }

    private void notifyServerFinished(PartitionTask partitionTask) {
        WorkerMessage message = new WorkerMessage(WorkerMessageType.FINISHED_TASK, JSON.toJSONString(partitionTask), Settings.getWorkerIp());
        ServerChannel.send(message);
    }
}
