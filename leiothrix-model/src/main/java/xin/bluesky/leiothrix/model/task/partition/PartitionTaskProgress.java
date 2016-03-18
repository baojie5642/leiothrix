package xin.bluesky.leiothrix.model.task.partition;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

/**
 * @author 张轲
 */
public class PartitionTaskProgress {

    private PartitionTask partitionTask;

    private long endIndex;

    private ExecutionStatistics statistics;

    public PartitionTaskProgress() {
    }

    public PartitionTaskProgress(PartitionTask partitionTask, long endIndex, ExecutionStatistics statistics) {
        this.partitionTask = partitionTask;
        this.endIndex = endIndex;
        this.statistics = statistics;
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

    public ExecutionStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(ExecutionStatistics statistics) {
        this.statistics = statistics;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
