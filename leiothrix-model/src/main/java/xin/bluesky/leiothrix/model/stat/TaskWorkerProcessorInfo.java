package xin.bluesky.leiothrix.model.stat;

import java.util.Date;

/**
 * @author 张轲
 */
public class TaskWorkerProcessorInfo {

    private String taskId;

    private String workerIp;

    private String processorId;

    private Date time = new Date();

    public TaskWorkerProcessorInfo() {
    }

    public TaskWorkerProcessorInfo(String taskId, String workerIp, String processorId) {
        this.taskId = taskId;
        this.workerIp = workerIp;
        this.processorId = processorId;
    }

    public String getWorkerIp() {
        return workerIp;
    }

    public void setWorkerIp(String workerIp) {
        this.workerIp = workerIp;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }
}
