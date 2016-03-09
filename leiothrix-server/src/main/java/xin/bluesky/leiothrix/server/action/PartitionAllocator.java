package xin.bluesky.leiothrix.server.action;

import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.action.exception.NoTaskException;
import xin.bluesky.leiothrix.server.action.exception.WaitAndTryLaterException;

import java.util.List;

/**
 * 片划分的抽象接口.
 *
 * @author 张轲
 * @date 16/2/3
 */
public interface PartitionAllocator {

    List<PartitionTask> findRange(String taskId) throws NoTaskException, WaitAndTryLaterException;

}
