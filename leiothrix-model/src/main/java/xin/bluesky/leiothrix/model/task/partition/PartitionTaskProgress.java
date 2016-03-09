package xin.bluesky.leiothrix.model.task.partition;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * @author 张轲
 */
public class PartitionTaskProgress {

    private PartitionTask partitionTask;

    private long endIndex;

    public PartitionTaskProgress() {
    }

    public PartitionTaskProgress(PartitionTask partitionTask, long endIndex) {
        this.partitionTask = partitionTask;
        this.endIndex = endIndex;
    }

    public PartitionTask getPartitionTask() {
        return partitionTask;
    }

    public void setPartitionTask(PartitionTask partitionTask) {
        this.partitionTask = partitionTask;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
