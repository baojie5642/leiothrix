package xin.bluesky.leiothrix.server.bean.node;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import xin.bluesky.leiothrix.common.net.exception.CommandException;
import xin.bluesky.leiothrix.server.interactive.worker.CommandFactory;

import java.io.IOException;

import static org.apache.commons.lang3.builder.ToStringStyle.JSON_STYLE;

/**
 * @author 张轲
 * @date 16/1/20
 */
public class NodePhysicalInfo {

    private int cpuNumber;

    private long memoryTotal;

    private long memoryFree;

    /**
     * 在机器成为worker之前的空闲内存,这作为分配worker进程的上限.因为在该机上还可能有其余应用的进程,尽量避免资源挤用
     */
    private long memoryFreeBeforeAsWorker;

    private String user;

    private String ip;

    public NodePhysicalInfo() {
    }

    public NodePhysicalInfo(String user, String ip) {
        this.user = user;
        this.ip = ip;
    }

    public int getCpuNumber() {
        return cpuNumber;
    }

    public NodePhysicalInfo setCpuNumber(int cpuNumber) {
        this.cpuNumber = cpuNumber;
        return this;
    }

    public long getMemoryTotal() {
        return memoryTotal;
    }

    public NodePhysicalInfo setMemoryTotal(long memoryTotal) {
        this.memoryTotal = memoryTotal;
        return this;
    }

    public long getMemoryFree() {
        return memoryFree;
    }

    public NodePhysicalInfo setMemoryFree(long memoryFree) {
        this.memoryFree = memoryFree;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public NodePhysicalInfo setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public String getUser() {
        return user;
    }

    public NodePhysicalInfo setUser(String user) {
        this.user = user;
        return this;
    }

    public long getMemoryFreeBeforeAsWorker() {
        return memoryFreeBeforeAsWorker;
    }

    public void setMemoryFreeBeforeAsWorker(long memoryFreeBeforeAsWorker) {
        this.memoryFreeBeforeAsWorker = memoryFreeBeforeAsWorker;
    }

    public void retrieveCpuInfo() {
        try {
            Process process = Runtime.getRuntime().exec(CommandFactory.getRemoteFullCommandByKey("cpu.number", this.user, this.ip));
            String cpuNumber = IOUtils.toString(process.getInputStream()).replace("\n", "");
            this.setCpuNumber(Integer.parseInt(cpuNumber));
        } catch (IOException e) {
            throw new CommandException(String.format("获取物理机[ip=%s]的cpu信息时失败", this.getIp()));
        }
    }

    public void retrieveMemoryInfo() {
        try {
            Process process = Runtime.getRuntime().exec(CommandFactory.getRemoteFullCommandByKey("memory.info", this.user, this.ip));
            String memoryInfo = IOUtils.toString(process.getInputStream()).replace("\n", "");
            String[] arr = memoryInfo.split(",");
            int usedMemory = Integer.parseInt(arr[0]);
            int freeMemory = Integer.parseInt(arr[1]);
            this.setMemoryTotal(usedMemory + freeMemory);
            this.setMemoryFree(freeMemory);
        } catch (IOException e) {
            throw new CommandException(String.format("获取物理机[ip=%s]的内存信息时失败", this.getIp()));
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, JSON_STYLE).append("ip", ip)
                .append("cpu核数:", cpuNumber)
                .append("物理内存:", (memoryTotal >> 10) + "m")
                .append("可用内存:", (memoryFree >> 10) + "m")
                .toString();
    }
}
