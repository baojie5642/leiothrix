package xin.bluesky.leiothrix.server.bean.task;

import org.apache.commons.lang3.builder.CompareToBuilder;

import java.util.List;

/**
 * @author 张轲
 */
public class TaskRanges {
    private String taskId;

    private String tableName;

    private List<String> rangeNameList;

    public TaskRanges(String taskId, String tableName, List<String> rangeNameList) {
        this.taskId = taskId;
        this.tableName = tableName;
        this.rangeNameList = rangeNameList;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getRangeNameList() {
        return rangeNameList;
    }

    private class Range implements Comparable<Range> {
        private String name;

        private long startIndex;

        private long endIndex;

        public Range(String name, long startIndex, long endIndex) {
            this.name = name;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public String getName() {
            return name;
        }

        public long getStartIndex() {
            return startIndex;
        }

        public long getEndIndex() {
            return endIndex;
        }

        @Override
        public int compareTo(Range o) {
            return new CompareToBuilder().append(this.name, o.name).build();
        }
    }
}
