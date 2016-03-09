package xin.bluesky.leiothrix.test;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;

import java.util.UUID;

/**
 * @author 张轲
 * @date 16/2/23
 */
public class FunctionalTest extends BaseIntegrationTest {

    public static final Logger logger = LoggerFactory.getLogger(FunctionalTest.class);

    @Override
    @Before
    public void setUp() throws Exception {
        setServerConfig();
        super.setUp();
    }

    @Test
    public void test() throws Exception {
        // given
        final String taskId = UUID.randomUUID().toString();
        TaskStorage.createEmptyTask(taskId);
        TaskStorage.setJarPath(taskId, "/jar");
        TaskStorage.setMainClass(taskId, "MainClass");
        TaskStorage.setConfig(taskId, IOUtils.toString(getClass().getResourceAsStream("/config.json")));

        final String table1 = "t_user";
        final String table2 = "t_user_login_history";
        TableStorage.createTable(taskId, new TableMeta(table1, "id"));
        TableStorage.createTable(taskId, new TableMeta(table2, "id"));

        RangeStorage.createRange(taskId, table1, "0-10");
        RangeStorage.createRange(taskId, table1, "11-20");
        RangeStorage.createRange(taskId, table2, "0-10");
        RangeStorage.createRange(taskId, table2, "11-20");

        setServerInfo("localhost");
        setTaskId(taskId);

        Thread workerThread = startWorker();
        while (workerThread.isAlive()) {
            Thread.sleep(1000);
        }
    }

    private static void setServerConfig() throws Exception {
        ServerConfigure.setProperty("server", "localhost");
        ServerConfigure.setProperty("workers", "192.168.56.11");
    }

}
