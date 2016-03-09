package xin.bluesky.leiothrix.worker.action;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;
import xin.bluesky.leiothrix.common.net.NetUtils;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.msg.WorkerMessageType;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
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

    //todo: 可以考虑给application传递该参数的接口
    //worker中的每个工作线程,针对拿到的任务片,还需要分页查询,该属性指定了每页的数据条数
    private static final int WORKER_QUERY_RANGE = 10000;

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
            long endIndex = startIndex + WORKER_QUERY_RANGE - 1;
            if (endIndex > partitionTask.getRowEndIndex()) {
                executePage(partitionTask, jdbcTemplate, startIndex, partitionTask.getRowEndIndex());
                //最后一片任务不报告,以避免报告的消息在任务片结束的消息之后到达server,导致该range的状态不对
                //progressReporter.reportProgress(partitionTask, partitionTask.getRowEndIndex());
                break;
            } else {
                executePage(partitionTask, jdbcTemplate, startIndex, endIndex);
                //logger.debug("执行分页查询:tableName:{},startIndex:{},endIndex:{}", partitionTask.getTaskId(), startIndex, endIndex);
                progressReporter.reportProgress(partitionTask, endIndex);
                startIndex = endIndex + 1;
            }
        }
    }

    private void executePage(PartitionTask partitionTask, JdbcTemplate jdbcTemplate, long startIndex, long endIndex) {
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
        logger.info("本次任务片分页查询[table={},startIndex={},endIndex={}]查询结束,有{}行数据,耗时{}毫秒",
                partitionTask.getTableName(), partitionTask.getRowStartIndex(),
                partitionTask.getRowEndIndex(), result.size(), stopWatch.getTime());

        DatabasePageDataHandler databasePageDataHandler = Settings.getConfiguration().getDatabasePageDataHandler();
        try {
            databasePageDataHandler.handle(partitionTask.getTableName(), result);
        } catch (Exception e) {
            databasePageDataHandler.exceptionCaught(partitionTask.getTableName(), result, e);
        }
    }

    private int calQueryPage(PartitionTask partitionTask) {
        int page = (int) ((partitionTask.getRowEndIndex() - partitionTask.getRowStartIndex()) / WORKER_QUERY_RANGE);
        long balance = (partitionTask.getRowEndIndex() - partitionTask.getRowStartIndex()) % WORKER_QUERY_RANGE;
        if (balance != 0) {
            page = page + 1;
        }
        return page;
    }

    private void notifyServerFinished(PartitionTask partitionTask) {
        WorkerMessage message = new WorkerMessage(WorkerMessageType.FINISHED_TASK, JSON.toJSONString(partitionTask), NetUtils.getLocalIp());
        ServerChannel.send(message);
    }
}
