package xin.bluesky.leiothrix.test;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static xin.bluesky.leiothrix.server.storage.ServerStorage.SERVER;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.createNode;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.makePath;

/**
 * @author 张轲
 */
@Ignore("通过日志来看worker是否收到了server发来的变更消息")
public class ServerUpdatedTest extends BaseIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(ServerUpdatedTest.class);

    @Test
    public void should_trigger_server_updated() throws Exception {
        // given
        setServerInfo("localhost");
        startWorker();

        // when
        final String newServer = "192.168.1.101";
        createNode(makePath(SERVER, newServer));
        logger.info("增加一个server节点[ip={}]", newServer);

        Thread.sleep(5 * 1000);
    }

}