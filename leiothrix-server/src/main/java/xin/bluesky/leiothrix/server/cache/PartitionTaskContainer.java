package xin.bluesky.leiothrix.server.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.task.partition.PartitionTask;
import xin.bluesky.leiothrix.server.storage.RangeStorage;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author 张轲
 */
public class PartitionTaskContainer {

    private static final Logger logger = LoggerFactory.getLogger(PartitionTaskContainer.class);

    private static Map<String, PartitionTaskContainer> map = new ConcurrentHashMap<>();

    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("partition-task-cache-%d").build());

    private LinkedBlockingDeque<PartitionTask> queue = new LinkedBlockingDeque();

    static {
        executorService.scheduleAtFixedRate(new UpdateOwnership(), 18, 15, TimeUnit.SECONDS);
    }

    public synchronized static PartitionTaskContainer getInstance(String taskId) {
        if (map.get(taskId) == null) {
            map.put(taskId, new PartitionTaskContainer());
        }
        return map.get(taskId);
    }

    public synchronized static void evict(String taskId) {
        map.remove(taskId);
    }

    private PartitionTaskContainer() {
    }

    public PartitionTask poll() {
        return queue.poll();
    }

    public void offer(List<PartitionTask> list) {
        list.forEach(t -> {
            queue.offer(t);
        });
    }

    public void offer(PartitionTask task) {
        queue.offer(task);
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    private static class UpdateOwnership implements Runnable {
        @Override
        public void run() {

            Set<String> keySet = new HashSet(map.keySet());
            keySet.forEach(key -> {
                PartitionTaskContainer container = map.get(key);
                if (container.size() > 0) {
                    logger.info("当前有{}个任务片尚未分配", container.size());
                }
                for (Iterator<PartitionTask> iterator = container.queue.iterator(); iterator.hasNext(); ) {
                    // 刷新节点更新时间,表明当前server仍然掌握着该range
                    PartitionTask pt = iterator.next();
                    RangeStorage.refreshRangeLastUpdateTime(pt.getTaskId(), pt.getTableName(), pt.getRangeName());
                    logger.debug("刷新range[taskId={},tableName={},rangeName={}]的更新时间", pt.getTaskId(), pt.getTableName(), pt.getRangeName());
                }
            });
        }
    }
}
