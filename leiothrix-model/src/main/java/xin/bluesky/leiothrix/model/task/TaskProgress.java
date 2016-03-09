package xin.bluesky.leiothrix.model.task;

/**
 * @author 张轲
 */
public class TaskProgress {

    private String status;

    private String desc;

    public TaskProgress() {
    }

    public TaskProgress(String status) {
        this.status = status;
    }

    public TaskProgress(String status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
