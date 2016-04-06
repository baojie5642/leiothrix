package xin.bluesky.leiothrix.server.storage.zk;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;

/**
 * @author 张轲
 */
public class ZookeeperUtils {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperUtils.class);

    public static boolean checkExists(String path) {
        Preconditions.checkNotNull(path, "路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上查询路径[%s]是否存在时失败", path), e);
        }
    }

    public static Stat getNodeStat(String nodePath) {
        Preconditions.checkNotNull(nodePath, "节点路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            return client.checkExists().forPath(nodePath);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上获得节点[%s]信息时失败", nodePath), e);
        }
    }

    public static void mkdir(String dirPath) {
        Preconditions.checkNotNull(dirPath, "节点路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            if (client.checkExists().forPath(dirPath) == null) {
                ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), dirPath);
            }
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上创建目录[%s]失败", dirPath), e);
        }
    }

    public static String mkdir(String parentPath, String dirName) {
        Preconditions.checkNotNull(parentPath, "父路径不能为空");
        Preconditions.checkNotNull(dirName, "目录名称不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            String path = ZKPaths.makePath(parentPath, dirName);
            if (client.checkExists().forPath(path) == null) {
                ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), path);
            }
            return path;
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上创建目录[%s](父目录为%s)失败", dirName, parentPath), e);
        }
    }

    public static void mkdirRecursive(String dirName) {
        Preconditions.checkNotNull(dirName, "目录名称不能为空");

        int pos = dirName.indexOf("/");
        if (pos == -1) {
            return;
        }

        String[] array = dirName.split("/");
        String path = "";
        for (int i = 0; i < array.length; i++) {
            if (StringUtils.isBlank(array[i])) {
                continue;
            }
            path = path + "/" + array[i];
            mkdir(path);
        }
    }

    public static void createNode(String nodePath) {
        createNode(nodePath, CreateMode.PERSISTENT);
    }

    public static void createNode(String nodePath, CreateMode mode) {
        Preconditions.checkNotNull(nodePath, "节点路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            if (client.checkExists().forPath(nodePath) == null) {
                client.create().creatingParentsIfNeeded().withMode(mode).forPath(nodePath);
            }
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上创建节点[%s]失败", nodePath), e);
        }
    }

    public static void createNodeAndSetData(String parentPath, String nodeName, String data) {
        createNodeAndSetData(makePath(parentPath, nodeName), data);
    }

    public static void createNodeAndSetData(String path, String data) {
        Preconditions.checkNotNull(path, "节点名称不能为空");
        Preconditions.checkNotNull(data, "节点数据不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
            } else {
                client.setData().forPath(path, data.getBytes());
            }
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上创建并给节点赋值[%s]失败", path), e);
        }
    }


    public static void setData(String nodePath, String data) {
        Preconditions.checkNotNull(nodePath, "节点路径不能为空");
        Preconditions.checkNotNull(data, "节点数据不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            if (client.checkExists().forPath(nodePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(nodePath);
            }
            client.setData().forPath(nodePath, data.getBytes());
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上给节点[%s]赋值[%s]时失败", nodePath, data), e);
        }
    }

    public static void delete(String nodePath) {
        Preconditions.checkNotNull(nodePath, "节点路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            if (checkExists(nodePath)) {
                client.delete().deletingChildrenIfNeeded().forPath(nodePath);
            }
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上删除节点[%s]失败", nodePath), e);
        }
    }

    public static List<String> getChildrenWithSimplePath(String dirPath) {
        Preconditions.checkNotNull(dirPath, "目录路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            if (client.checkExists().forPath(dirPath) == null) {
                return new ArrayList();
            }

            return client.getChildren().forPath(dirPath);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上获得该路径[%s]的子节点失败", dirPath), e);
        }
    }

    public static List<String> getChildrenWithFullPath(String dirPath) {
        List<String> childrenWithSimplePath = getChildrenWithSimplePath(dirPath);

        return from(childrenWithSimplePath).transform(childPath -> {
            return makePath(dirPath, childPath);
        }).toList();
    }

    public static String getDataString(String nodePath) {
        return new String(getDataByte(nodePath));
    }

    public static Integer getDataInteger(String nodePath) {
        return Integer.parseInt(getDataString(nodePath));
    }

    public static Long getDataLong(String nodePath) {
        return Long.parseLong(getDataString(nodePath));
    }

    public static Boolean getDataBoolean(String nodePath) {
        return Boolean.parseBoolean(getDataString(nodePath));
    }

    public static byte[] getDataByte(String nodePath) {
        Preconditions.checkNotNull(nodePath, "节点路径不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            return client.getData().forPath(nodePath);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上获得该节点[%s]的数据失败", nodePath), e);
        }
    }

    public static String makePath(String... names) {
        Preconditions.checkNotNull(names.length, "节点名称不能为空");

        try {
            CuratorFramework client = ZookeeperClientFactory.get();
            String totalPath = "";
            for (int i = 0; i < names.length; i++) {
                totalPath = ZKPaths.makePath(totalPath, names[i]);
            }
            return totalPath;
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new ZookeeperException(String.format("在zookeeper上创建路径失败"), e);
        }
    }

    public static CuratorFramework getClient() {
        return ZookeeperClientFactory.get();
    }

    /**
     * 该方法不是线程安全的
     *
     * @param nodePath  node path
     * @param increment the value increased
     */
    public static void addIntValue(String nodePath, Integer increment) {
        if (!checkExists(nodePath)) {
            createNodeAndSetData(nodePath, String.valueOf(increment));
        } else {
            int existed = getDataInteger(nodePath);
            int newValue = existed + increment;
            setData(nodePath, String.valueOf(newValue));
        }
    }

    /**
     * 该方法不是线程安全的
     *
     * @param nodePath  node path
     * @param increment the value increased
     */
    public static void addLongValue(String nodePath, Long increment) {
        if (!checkExists(nodePath)) {
            createNodeAndSetData(nodePath, String.valueOf(increment));
        } else {
            long existed = getDataLong(nodePath);
            long newValue = existed + increment;
            setData(nodePath, String.valueOf(newValue));
        }
    }

    /**
     * 该方法不是线程安全的
     *
     * @param nodePath node path
     * @param data     data appended to the node's content
     */
    public static void appendValue(String nodePath, String data) {
        if (!checkExists(nodePath)) {
            createNodeAndSetData(nodePath, data);
        } else {
            StringBuffer buffer = new StringBuffer(getDataString(nodePath));
            // 如果内容大于1M,就失败,为了防止有计算误差,这里取500k
            if (buffer.length() > 500 * 1024 * 1024) {
                return;
            }
            buffer.append(data);
            setData(nodePath, buffer.toString());
        }
    }
}
