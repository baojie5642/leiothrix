package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.support.MockChannelHandlerContext;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;
import xin.bluesky.leiothrix.model.db.DialectType;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.task.TaskConfig;

import java.util.UUID;

import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.ACQUIRE_TASK;

/**
 * @author 张轲
 */
public class AcquireTaskHandlerTest extends StorageContainerDependency {

    private AcquireTaskHandler handler = new AcquireTaskHandler();

    @Test
    public void should_acquire_task_correct() throws Exception {
        // given
        String taskId = createTask();

        // when
        MockChannelHandlerContext ctx = new MockChannelHandlerContext();
        final WorkerMessage workerMessage = new WorkerMessage(ACQUIRE_TASK, taskId, "localhost");
        handler.handle(ctx, workerMessage);
        String content = ctx.getWriteContent();

        // then
        System.out.println(content);
        handler.handle(ctx, workerMessage);
    }

    private String createTask() {
        String taskId = UUID.randomUUID().toString();
        TaskStorage.createEmptyTask(taskId);

        TaskStorage.setJarPath(taskId, "/jar");

        TaskStorage.setMainClass(taskId, "MainClass");

        DatabaseInfo db1 = new DatabaseInfo(DialectType.MYSQL, "localhost", 3306, "test1", "user", "password");
        DatabaseInfo db2 = new DatabaseInfo(DialectType.MYSQL, "localhost", 3310, "test2", "user", "password");
        TaskConfig taskConfig = new TaskConfig();
        taskConfig.setDatabaseInfoList(ImmutableList.of(db1, db2));
        TaskStorage.setConfig(taskId, JSON.toJSONString(taskConfig));

        TableMeta table1Meta = new TableMeta("table-1", "id");
        TableMeta table2Meta = new TableMeta("table-2", "id");

        TableStorage.createTable(taskId, table1Meta);
        TableStorage.createTable(taskId, table2Meta);

        RangeStorage.createRange(taskId, "table-1", "0-100");
        RangeStorage.createRange(taskId, "table-1", "101-200");

        RangeStorage.createRange(taskId, "table-2", "0-1000");
        RangeStorage.createRange(taskId, "table-2", "1001-2000");
        RangeStorage.createRange(taskId, "table-2", "2001-3000");

        return taskId;
    }

}