package xin.bluesky.leiothrix.server.storage.zk;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.Properties;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.checkExists;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.delete;

/**
 * @author 张轲
 */
public class EmbeddedZookeeperServer {

    public static final Logger logger = LoggerFactory.getLogger(EmbeddedZookeeperServer.class);

    private static ZooKeeperServerMain server;

    private EmbeddedZookeeperServer() {
    }

    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("线程[id={},name={}]发生异常:{}", t.getId(), t.getName(), getStackTrace(e));
        });
    }

    public synchronized static void start() throws Exception {
        if (server != null) {
            return;
        }
        Properties properties = new Properties();
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        try {
            properties.load(EmbeddedZookeeperServer.class.getResourceAsStream("/zoo.cfg"));
            quorumPeerConfig.parseProperties(properties);
        } catch (Exception e) {
            logger.error("读取embedded zookeeper配置文件时发生异常", e);
            throw e;
        }

        server = new ZooKeeperServerMain();
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumPeerConfig);

        new Thread(() -> {
            try {
                server.runFromConfig(serverConfig);
                Thread.sleep(3 * 1000);//等待server启动完毕
                clean();
            } catch (Exception e) {
                logger.error("启动embedded zookeeper server时发生异常", e);
            }
        }).start();
    }

    public static void clean() {
        if (checkExists(TaskStorage.TASKS)) {
            delete(TaskStorage.TASKS);
        }
    }
}
