package xin.bluesky.leiothrix.server.support;

import org.junit.After;
import org.junit.BeforeClass;
import xin.bluesky.leiothrix.server.storage.zk.EmbeddedZookeeperServer;

/**
 * @author 张轲
 */
public class StorageContainerDependency {

    @BeforeClass
    public static void before() throws Exception {
        EmbeddedZookeeperServer.start();
    }

    @After
    public void tearDown() throws Exception {
        EmbeddedZookeeperServer.clean();
    }
}
