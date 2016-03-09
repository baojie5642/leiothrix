package xin.bluesky.leiothrix.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.Server;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.storage.zk.EmbeddedZookeeperServer;
import xin.bluesky.leiothrix.worker.WorkerConfiguration;
import xin.bluesky.leiothrix.worker.WorkerProcessor;
import xin.bluesky.leiothrix.worker.api.DatabasePageDataHandler;

import java.net.URISyntaxException;
import java.util.List;

/**
 * @author 张轲
 */
public class BaseIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(BaseIntegrationTest.class);

    private static Server server;

    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("线程[id={},name={}]出现异常:{}", t.getId(), t.getName(), ExceptionUtils.getStackTrace(e));
        });
    }

    @Before
    public void setUp() throws Exception {
        initServerConfig();

        startServer();
    }

    private void initServerConfig() throws URISyntaxException {
        final String configFilePath = ClassLoader.getSystemClassLoader().getResource("config.properties").toURI().getPath();
        System.setProperty(Constant.SYS_PROP_CONFIG_FILE, configFilePath);

        System.setProperty("cleanZkDataAfterStart", "true");
        System.setProperty("worker.compensate.enable", "false");
        System.setProperty("worker.processor.threadnum.factor", "1");
    }

    private void startServer() throws InterruptedException {
        new Thread(() -> {
            if (server == null) {
                server = new Server();
                try {
                    server.start();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }).start();
        Thread.sleep(10 * 1000);//等待server启动完毕
    }

    @After
    public void tearDown() throws Exception {
        EmbeddedZookeeperServer.clean();
    }

    protected void setServerInfo(String serverIp) {
        System.setProperty("server.ip", serverIp);
        System.setProperty("server.port", ServerConfigure.get("server.port.worker"));
    }

    protected void setTaskId(String taskId) {
        System.setProperty("taskId", taskId);
    }

    protected Thread startWorker() throws Exception {
        logger.info("开始执行{}", BaseIntegrationTest.class);
        final Thread workerThread = new Thread(new DummyWorker());
        workerThread.start();
        Thread.sleep(3 * 1000);//等待worker启动完成
        return workerThread;
    }

    private static class DummyWorker implements Runnable {
        @Override
        public void run() {
            WorkerConfiguration configuration = new WorkerConfiguration();
            configuration.setDatabasePageDataHandler(new DatabasePageDataHandler() {
                @Override
                public void handle(String tableName, List<JSONObject> dataList) throws Exception {
                    logger.info("查询表[{}]的任务表,得到{}条数据", tableName, dataList.size());
                }

                @Override
                public void exceptionCaught(String tableName, List<JSONObject> dataList, Exception e) {
                    logger.error("处理表[{}]数据时发生异常,数据为:{},异常为:{}",
                            tableName, CollectionsUtils2.toString(dataList), ExceptionUtils.getStackTrace(e));
                }
            });

            WorkerProcessor bootStrap = new WorkerProcessor(configuration);

            try {
                bootStrap.start();
            } catch (Exception e) {
                logger.error("worker启动失败,异常信息:{}", ExceptionUtils.getStackTrace(e));
            }
        }
    }
}
