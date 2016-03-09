package xin.bluesky.leiothrix.model.bin;

/**
 * @author 张轲
 */
public class SubmitResponse {

    private String taskId;

    private SubmitStatus status;

    private String desc;

    public SubmitResponse() {
    }

    public SubmitResponse(String taskId, SubmitStatus status) {
        this.taskId = taskId;
        this.status = status;
    }

    public SubmitResponse(String taskId, SubmitStatus status, String desc) {
        this.taskId = taskId;
        this.status = status;
        this.desc = desc;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public SubmitStatus getStatus() {
        return status;
    }

    public void setStatus(SubmitStatus status) {
        this.status = status;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
