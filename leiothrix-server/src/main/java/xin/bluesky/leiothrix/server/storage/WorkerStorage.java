package xin.bluesky.leiothrix.server.storage;

import com.alibaba.fastjson.JSON;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils;

import java.util.List;

import static xin.bluesky.leiothrix.server.storage.zk.ZookeeperUtils.*;

/**
 * @author 张轲
 */
public class WorkerStorage {

    public static final String WORKERS = Constant.ROOT_DIR + "/workers";

    private static final String NAME_WORKER_INFO = "info";

    public static final String NAME_WORKER_PROCESSOR_NUMBER = "workerProcessorNumber";

    public static void savePhysicalInfo(NodePhysicalInfo worker) {
        String path = getWorkerPath(worker.getIp());
        ZookeeperUtils.setData(path, JSON.toJSONString(worker));
    }

    private static String getWorkerPath(String workerIp) {
        return makePath(WORKERS, workerIp, NAME_WORKER_INFO);
    }

    public static NodePhysicalInfo getPhysicalInfo(String workerIp) {
        String path = getWorkerPath(workerIp);
        return JSON.parseObject(getDataString(path), NodePhysicalInfo.class);
    }

    public static void delete(String workerIp) {
        ZookeeperUtils.delete(makePath(WORKERS, workerIp));
    }

    public static List<String> getAllWorkers() {
        return ZookeeperUtils.getChildrenWithSimplePath(WORKERS);
    }

    public static Integer getWorkersNumber(String workerIp) {
        String workerNumberPath = makePath(WORKERS, workerIp, NAME_WORKER_PROCESSOR_NUMBER);
        if (!checkExists(workerNumberPath)) {
            return 0;
        } else {
            return getDataInteger(workerNumberPath);
        }
    }

    public static void setWorkersNumber(String workerIp, int newNumber) {
        String workerNumberPath = makePath(WORKERS, workerIp, NAME_WORKER_PROCESSOR_NUMBER);
        setData(workerNumberPath, String.valueOf(newNumber));
    }

    public static boolean alreadyRegister() {
        return checkExists(WORKERS);
    }

    public static boolean exist(String workerIp) {
        return checkExists(makePath(WORKERS, workerIp));
    }

    public static boolean notExist(String workerIp) {
        return !exist(workerIp);
    }
}
