package xin.bluesky.leiothrix.server.storage;

import org.apache.zookeeper.Watcher;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.action.ServerUpdatedTrigger;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperException;

import java.util.List;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.*;

/**
 * @author 张轲
 */
public class ServerStorage {
    public static final String SERVER = Constant.ROOT_DIR + "/server";

    /**
     * 在zk上给server注册EPHEMERAL类型的节点.如果已经存在,则先删后建.
     *
     * @param ip server ip
     */
    public static void registerServer(String ip) {
        String path = makePath(SERVER, ip);

        try {
            //server关闭之后,但zk没有过期之前,这里的create会失败.这主要发生在server重启过程中
            if (checkExists(path)) {
                delete(path);
            }
            getClient().create().creatingParentsIfNeeded().withMode(EPHEMERAL).forPath(path);
            getClient().getChildren().watched().forPath(SERVER);
        } catch (Exception e) {
            throw new ZookeeperException(String.format("在zookeeper上创建server节点[%s]失败", path), e);
        }
    }

    public static void addServerListWatch(ServerUpdatedTrigger trigger) {
        getClient().getCuratorListenable().addListener(((client, event) -> {
            if (event.getWatchedEvent().getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                if (SERVER.equals(event.getPath())) {
                    trigger.trigger(client.getChildren().forPath(SERVER));
                }
            }

        }));
    }

    public static List<String> getAllServers() {
        return getChildrenWithSimplePath(SERVER);
    }
}
