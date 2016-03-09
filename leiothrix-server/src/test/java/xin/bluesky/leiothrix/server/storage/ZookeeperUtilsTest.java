package xin.bluesky.leiothrix.server.storage;

import org.apache.zookeeper.CreateMode;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.storage.zk.EmbeddedZookeeperServer;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author 张轲
 */
public class ZookeeperUtilsTest {

    @BeforeClass
    public static void before() throws Exception {
        EmbeddedZookeeperServer.start();
    }

    @After
    public void tearDown() throws Exception {
        EmbeddedZookeeperServer.clean();
    }

    /**
     * ˀ
     * **********************测试节点是否存在***********************************************
     */
    @Test
    public void should_return_true_if_node_exist() throws Exception {
        // given
        String nodePath = Constant.ROOT_DIR + "/existnode";

        // when
        ZookeeperUtils.createNode(nodePath);

        // then
        Assert.assertThat(ZookeeperUtils.checkExists(nodePath), Is.is(true));
    }

    @Test
    public void should_return_false_if_node_not_exist() throws Exception {
        // given
        String nodePath = Constant.ROOT_DIR + "/notexist";

        // then
        Assert.assertThat(ZookeeperUtils.checkExists(nodePath), Is.is(false));
    }

    /**
     * **********************测试删除节点路径***********************************************
     */
    @Test
    public void should_delete_cascade_success() throws Exception {
        // given
        String topDir = Constant.ROOT_DIR + "/cascade";
        String secondLevelDir = "/second";
        String thirdLevelDir = "/third";

        ZookeeperUtils.mkdir(topDir);
        String secondLevelFullDir = ZookeeperUtils.mkdir(topDir, secondLevelDir);
        ZookeeperUtils.createNodeAndSetData(secondLevelFullDir, "name", "secondLevel");
        String thirdLevelFullDir = ZookeeperUtils.mkdir(secondLevelFullDir, thirdLevelDir);
        ZookeeperUtils.createNodeAndSetData(thirdLevelFullDir, "name", "thirdLevel");

        // when
        ZookeeperUtils.delete(topDir);

        // then
        Assert.assertThat(ZookeeperUtils.checkExists(topDir), Is.is(false));
    }

    /**
     * **********************测试获得子节点的名称***********************************************
     */
    @Test
    public void should_get_children_simple_name_success() throws Exception {
        // given
        String topDir = Constant.ROOT_DIR + "/cascade";

        ZookeeperUtils.mkdir(topDir);
        ZookeeperUtils.createNodeAndSetData(topDir, "name", "zhangke");
        ZookeeperUtils.createNodeAndSetData(topDir, "age", "35");

        // when
        List<String> list = ZookeeperUtils.getChildrenWithSimplePath(topDir);

        // then
        Assert.assertThat(list.contains("name"), Is.is(true));
        Assert.assertThat(list.contains(topDir + "/name"), Is.is(false));
    }

    /**
     * **********************测试获得子节点的完整路径名称***********************************************
     */
    @Test
    public void should_get_children_full_name_success() throws Exception {
        // given
        String topDir = Constant.ROOT_DIR + "/cascade";

        ZookeeperUtils.mkdir(topDir);
        ZookeeperUtils.createNodeAndSetData(topDir, "name", "zhangke");
        ZookeeperUtils.createNodeAndSetData(topDir, "age", "35");

        // when
        List<String> list = ZookeeperUtils.getChildrenWithFullPath(topDir);

        // then
        Assert.assertThat(list.contains("name"), Is.is(false));
        Assert.assertThat(list.contains(topDir + "/name"), Is.is(true));
    }

    /**
     * **********************测试创建目录***********************************************
     */
    @Test
    public void should_mkdir_recursive() throws Exception {
        // given
        String node = Constant.ROOT_DIR + "/mkdir/recursive/test";

        // when
        ZookeeperUtils.mkdirRecursive(node);

        // then
        Assert.assertThat(ZookeeperUtils.checkExists(node), Is.is(true));
    }

    /**
     * **********************测试创建节点***********************************************
     */
    @Test
    public void should_create_node_cascade() throws Exception {
        // given
        String node = Constant.ROOT_DIR + "/a/b/c";

        // when
        ZookeeperUtils.createNode(node);

        // then
        assertThat(ZookeeperUtils.checkExists(node), is(true));
    }

    @Test
    public void should_create_node_with_mode() throws Exception {
        // given
        String node1 = TaskStorage.TASKS + "/a/b/c";
        String node2 = TaskStorage.TASKS + "/a/b/d";

        // when
        ZookeeperUtils.createNode(node1, CreateMode.EPHEMERAL_SEQUENTIAL);
        ZookeeperUtils.createNode(node2, CreateMode.EPHEMERAL_SEQUENTIAL);

        // then
        assertThat(ZookeeperUtils.checkExists(node1 + "0000000000"), is(true));
        assertThat(ZookeeperUtils.checkExists(node2 + "0000000001"), is(true));
    }
}