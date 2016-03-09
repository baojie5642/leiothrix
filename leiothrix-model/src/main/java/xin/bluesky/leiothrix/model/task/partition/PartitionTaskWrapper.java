package xin.bluesky.leiothrix.model.task.partition;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * @author 张轲
 */
public class PartitionTaskWrapper {
    public static final String STATUS_SUCCESS = "success";

    public static final String STATUS_WAIT_AND_TRY_LATER = "waitAndTryLater";

    public static final String STATUS_NO_TASK = "noTask";

    public static final String STATUS_FAIL = "fail";

    private String status;

    private PartitionTask partitionTask;

    public PartitionTaskWrapper() {
    }

    public PartitionTaskWrapper(String status, PartitionTask partitionTask) {
        this.status = status;
        this.partitionTask = partitionTask;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public PartitionTask getPartitionTask() {
        return partitionTask;
    }

    public void setPartitionTask(PartitionTask partitionTask) {
        this.partitionTask = partitionTask;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
