package xin.bluesky.leiothrix.server.action;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.bean.node.NodePhysicalInfo;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.collect.FluentIterable.from;
import static xin.bluesky.leiothrix.common.net.NetUtils.pingSuccess;
import static xin.bluesky.leiothrix.common.net.NetUtils.sshSuccess;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class WorkerInfoInitializer {

    private static final Logger logger = LoggerFactory.getLogger(WorkerInfoInitializer.class);

    private static final Pattern pattern = Pattern.compile(",");

    public List<String> availableWorkers() {
        List<String> allWorkers = configuredWorkers();

        return from(allWorkers)
                .filter((worker) -> {
                    return pingSuccess(worker) && sshSuccess(worker, ServerConfigure.get("worker.user"));
                })
                .toList();
    }

    private List<String> configuredWorkers() {
        String workers = ServerConfigure.get("workers");

        if (StringUtils.isBlank(workers)) {
            return Lists.newArrayList("localhost");
        }

        return Arrays.asList(pattern.split(workers));
    }

    public List<NodePhysicalInfo> availableWorkersPhysicalInfo() {
        String remoteUser = ServerConfigure.get("worker.user");

        List<String> ips = availableWorkers();
        return from(ips)
                .transform((ip) -> {
                    NodePhysicalInfo info = new NodePhysicalInfo(remoteUser, ip);
                    info.retrieveCpuInfo();
                    info.retrieveMemoryInfo();
                    return info;
                })
                .toList();
    }

    public NodePhysicalInfo physicalInfo(String ip) {
        String remoteUser = ServerConfigure.get("worker.user");

        NodePhysicalInfo info = new NodePhysicalInfo(remoteUser, ip);
        info.retrieveCpuInfo();
        info.retrieveMemoryInfo();
        return info;
    }
}
