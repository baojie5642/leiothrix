package xin.bluesky.leiothrix.server.bean.task;

import java.util.List;

/**
 * @author 张轲
 */
public class TaskTables {

    private String taskId;

    private List<String> tableNameList;

    public TaskTables(String taskId, List<String> tableNameList) {
        this.taskId = taskId;
        this.tableNameList = tableNameList;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<String> getTableNameList() {
        return tableNameList;
    }
}
