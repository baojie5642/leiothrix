package xin.bluesky.leiothrix.server.storage;

import org.junit.Test;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.model.task.TaskStatus;

import java.util.List;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static xin.bluesky.leiothrix.server.storage.TaskStorage.NAME_STATUS;
import static xin.bluesky.leiothrix.server.storage.TaskStorage.TASKS;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.createNodeAndSetData;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.makePath;

/**
 * @author 张轲
 */
public class TaskStorageTest extends StorageContainerDependency {

    /**
     * ˀ
     * **********************测试获得未分配任务***********************************************
     */
    @Test
    public void should_get_unallocated_task() {
        // given
        createNodeAndSetData(makePath(TASKS, "processingTask"), NAME_STATUS, TaskStatus.PROCESSING.name());
        createNodeAndSetData(makePath(TASKS, "unallocatedTask"), NAME_STATUS, TaskStatus.UNALLOCATED.name());

        // when
        String taskId = TaskStorage.getUnallocatedTask();

        // then
        assertThat(taskId, is("unallocatedTask"));
    }

    @Test
    public void should_return_null_if_no_unallocated_task() {
        // given
        createNodeAndSetData(makePath(TASKS, "processingTask"), NAME_STATUS, TaskStatus.PROCESSING.name());
        createNodeAndSetData(makePath(TASKS, "task2"), NAME_STATUS, TaskStatus.PROCESSING.name());

        // when
        String taskId = TaskStorage.getUnallocatedTask();

        // then
        assertThat(taskId, nullValue());
    }

    /**
     * ˀ
     * **********************测试获得最老的任务***********************************************
     */
    @Test
    public void should_get_oldest_task() throws Exception {
        // given
        createNodeAndSetData(makePath(TASKS, "task1"), NAME_STATUS, TaskStatus.PROCESSING.name());
        Thread.sleep(1000);//等待1秒以消除网络延时差异
        createNodeAndSetData(makePath(TASKS, "task2"), NAME_STATUS, TaskStatus.PROCESSING.name());
        Thread.sleep(1000);
        createNodeAndSetData(makePath(TASKS, "task3"), NAME_STATUS, TaskStatus.PROCESSING.name());

        // when
        String taskId = TaskStorage.getOldestProcessingTask();

        // then
        assertThat(taskId, is("task1"));
    }

    /**
     * ˀ
     * **********************测试获得当前生效的任务***********************************************
     */
    @Test
     public void should_get_processing_task() {
        // given
        createNodeAndSetData(makePath(TASKS, "processingTask"), NAME_STATUS, TaskStatus.PROCESSING.name());
        createNodeAndSetData(makePath(TASKS, "unallocatedTask"), NAME_STATUS, TaskStatus.UNALLOCATED.name());
        createNodeAndSetData(makePath(TASKS, "finishedTask"), NAME_STATUS, TaskStatus.FINISHED.name());

        // when
        List<String> taskList = TaskStorage.getAllProcessingTasks();

        // then
        assertThat(taskList.size(), is(1));
    }

}