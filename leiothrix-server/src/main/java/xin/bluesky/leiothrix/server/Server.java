package xin.bluesky.leiothrix.server;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.server.action.ServerUpdatedTrigger;
import xin.bluesky.leiothrix.server.action.WorkerInfoInitializer;
import xin.bluesky.leiothrix.server.background.*;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.interactive.client.ClientRequestListener;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerMessageListener;
import xin.bluesky.leiothrix.server.storage.ServerStorage;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;
import xin.bluesky.leiothrix.server.storage.zk.EmbeddedZookeeperServer;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperClientFactory;
import xin.bluesky.leiothrix.server.util.LeiothrixUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

/**
 * server.
 *
 * @author 张轲
 */
public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private static final String LEADER_DIR = Constant.ROOT_DIR + "/leader";

    private Object serverLock = new Object();

    private Object leaderLock = new Object();

    private LeaderSelector selector;

    private static Server server;

    public static Server getServer() {
        return server;
    }

    public Server() {
        initLeaderSelector();
        Server.server = this;
    }

    private void initLeaderSelector() {
        CuratorFramework client = ZookeeperClientFactory.get();

        selector = new LeaderSelector(client, LEADER_DIR,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        logger.info("由当前server来启动执行后台服务");
                        try {

                            startBackgroundJob();

                            await(leaderLock, "退出");

                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                });
    }

    /**
     * server是无中心的集群模式
     *
     * @throws Exception if some bad thing happen
     */
    public void start() throws Exception {

        logger.info("开始启动server....");

        startZkServerIfNecessary();

        startListener();

        registerServer();

        registerWorkerIfNecessary();

        addWatch();

        addShutdownHook();

        selector.start();

        logger.info("server成功完成启动");

        await(serverLock, "server成功关闭");
    }

    private void startZkServerIfNecessary() throws Exception {
        String embeddedConfig = ServerConfigure.get("zookeeper.embedded");
        if ("true".equals(embeddedConfig)) {
            EmbeddedZookeeperServer.start();
            final String cleanZkDataAfterStart = System.getProperty("cleanZkDataAfterStart");
            if (StringUtils.isNotBlank(cleanZkDataAfterStart) && "true".equals(cleanZkDataAfterStart)) {
                logger.info("系统参数cleanZkDataAfterStart被设置为true,即将在启动server前清空zookeeper数据");
                EmbeddedZookeeperServer.clean();
            }
        }
    }

    /**
     * 由于zk中存有worker信息(除非server第一次启动),所以如果在运行过程中worker信息发生了变化(添加或移除或不可用),
     * 则将与config.properties中的信息不一致,导致再次启动server时,config中worker信息无效,故实施人员应该同步修改config.properties以保持一致
     */
    private void registerWorkerIfNecessary() {
        List<NodePhysicalInfo> newWorkers = new ArrayList();

        WorkerInfoInitializer workerInfoInitializer = new WorkerInfoInitializer();

        List<String> availableWorkers = workerInfoInitializer.availableWorkers();

        // 将配置中新增加的worker信息,注册到zk中去.而zk中已有的worker,信息更完整,不能动
        availableWorkers.forEach(ip -> {
            if (WorkerStorage.notExist(ip)) {
                NodePhysicalInfo info = workerInfoInitializer.physicalInfo(ip);
                info.setMemoryFreeBeforeAsWorker(info.getMemoryFree());
                WorkerStorage.savePhysicalInfo(info);
                newWorkers.add(info);
            }
        });

        if (newWorkers.size() > 0) {
            logger.info("注册worker信息成功,新增加worker:{}", "\r\n" + CollectionsUtils2.toString(newWorkers, "\r\n"));
        }
    }

    private void startListener() throws Exception {
        int clientListenerPort = ServerConfigure.get("server.port.client", Integer.class);
        ClientRequestListener.start(clientListenerPort);
        logger.info("启动客户端调用的http服务成功,监听端口{}", clientListenerPort);

        int workerListenerPort = ServerConfigure.get("server.port.worker", Integer.class);
        WorkerMessageListener.start(workerListenerPort);
        logger.info("启动worker调用的tcp服务成功,监听端口{}", workerListenerPort);
    }

    private void startBackgroundJob() {
        TaskStatusChecker.start();

        WorkerHealthChecker.start();

        TaskCompensatorJob.start();

        TimeoutPartitionTaskChecker.start();

        TaskDeleteJob.start();

        logger.info("启动后台服务成功,当前有:任务状态检查服务,worker健康检测服务,任务补偿服务,超时任务片检查服务,任务删除服务");
    }

    //todo:这里需要定时去注册吗?因为如server与zk的通讯中断了,就无法再注册上去,必须重启
    private void registerServer() {
        String serverIp = LeiothrixUtils.getMyIp();
        ServerStorage.registerServer(serverIp);
        logger.info("注册当前server:{}成功", CollectionsUtils2.toString(ServerStorage.getAllServers()));
    }

    private void addWatch() {
        ServerStorage.addServerListWatch(new ServerUpdatedTrigger());
        logger.info("添加watch成功");
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new ShutDownHook());
        logger.info("添加ShutdownHook成功");
    }

    private void await(Object o, String msg) {
        synchronized (o) {
            try {
                o.wait();
            } catch (Exception e) {
                logger.info(msg);
            }
        }
    }

    public void releaseLeaderSelector() {
        selector.close();
    }

    public static void main(String[] args) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("线程[id={},name={}]发生异常:{}", t.getId(), t.getName(), getStackTrace(e));
        });

        Server server = new Server();
        server.start();
    }
}
