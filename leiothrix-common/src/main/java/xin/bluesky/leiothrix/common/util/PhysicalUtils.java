package xin.bluesky.leiothrix.common.util;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.List;

/**
 * @author 张轲
 */
public class PhysicalUtils {

    /**
     * 获得CPU使用率
     */
    public static BigDecimal getCpuLoad() {
        OperatingSystemMXBean mxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return new BigDecimal(mxBean.getProcessCpuLoad());
    }

    /**
     * 获得永久代的GC次数,由于worker指定了G1回收策略,所以可以得到该值
     */
    public static long getOldGcTimes() {
        final List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean bean : mxBeans) {
            if (bean.getName().equals("G1 Old Generation")) {
                return bean.getCollectionCount();
            }
        }

        throw new RuntimeException("没有找到G1回收策略,是否没有以G1的策略启动worker?");
    }

    /**
     * 获得当前Java进程的PID
     *
     * @return
     */
    public static String getPid() {
        String processorId = ManagementFactory.getRuntimeMXBean().getName();
        if (processorId.indexOf("@") > 0) {
            processorId = processorId.substring(0, processorId.indexOf("@"));
        }
        return processorId;
    }
}
