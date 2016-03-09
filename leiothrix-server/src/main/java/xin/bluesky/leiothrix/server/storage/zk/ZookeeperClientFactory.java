package xin.bluesky.leiothrix.server.storage.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;

/**
 * @author 张轲
 * @date 16/1/19
 */
public class ZookeeperClientFactory {

    public static final String ROOT_PATH = "/leiothrix";

    private static CuratorFramework client;

    static {
        client = CuratorFrameworkFactory.newClient(ServerConfigure.get("zookeeper.address"), new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    public static CuratorFramework get() {
        return client;
    }
}
