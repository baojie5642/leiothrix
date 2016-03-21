package xin.bluesky.leiothrix.model.stat;

import java.util.Date;

/**
 * @author 张轲
 */
public class ProcessorInfo {

    public static final String STEP_STARTUP = "startUp";

    public static final String STEP_EXIT = "exit";

    private String taskId;

    private String workerIp;

    private String processorId;

    private String step;

    private boolean success;

    private String errorMsg;

    private Date time = new Date();

    public ProcessorInfo() {
    }

    public ProcessorInfo(String taskId, String workerIp, String processorId) {
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

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }
}
