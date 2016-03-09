package xin.bluesky.leiothrix.server.background.compensate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.action.WorkerProcessorLauncher;
import xin.bluesky.leiothrix.server.action.exception.NoResourceException;
import xin.bluesky.leiothrix.server.action.exception.NotAllowedLaunchException;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

/**
 * 补偿单个任务的策略.将空闲资源首先分配给未被执行的任务,如不存在则分配给最老的任务
 *
 * @author 张轲
 * @date 16/2/17
 */
public class CompensateSingleTaskStrategy implements CompensateStrategy {

    public static final Logger logger = LoggerFactory.getLogger(CompensateSingleTaskStrategy.class);

    private WorkerProcessorLauncher workerProcessorLauncher = new WorkerProcessorLauncher();

    @Override
    public void execute() {
        String taskId = null;

        try {
            // 首先获得之前由于资源不足,未得到执行的任务
            taskId = TaskStorage.getUnallocatedTask();
            if (StringUtils.isBlank(taskId)) {
                // 如果没有未执行任务,则获得最老的任务.如该任务当前资源已经足够,则不再需要补偿
                taskId = TaskStorage.getOldestProcessingTask();
            }
            if (StringUtils.isBlank(taskId)) {
                return;
            }
            if (TaskStorage.isResourceEnough(taskId)) {
                return;
            }

            workerProcessorLauncher.launch(taskId);
        } catch (NoResourceException e) {
            logger.info("当前所有资源都被分配完毕,没有可用资源来执行任务[taskId={}]", taskId);
        } catch (NotAllowedLaunchException e) {
            logger.info("当前最老的任务不允许被执行", e.getMessage());
        } catch (Exception e) {
            logger.error("执行任务[taskId={}]时失败,异常信息:{}", taskId, ExceptionUtils.getStackTrace(e));
        }
    }
}
