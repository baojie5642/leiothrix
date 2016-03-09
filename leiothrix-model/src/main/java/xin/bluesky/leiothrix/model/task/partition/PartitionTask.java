package xin.bluesky.leiothrix.model.task.partition;


import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;

/**
 * @author 张轲
 */
public class PartitionTask {

    private String taskId;

    private DatabaseInfo databaseInfo;

    private String tableName;

    private String primaryKey;

    private String partitionRangeName;

    private long rowStartIndex;

    private long rowEndIndex;

    private String columnNames;

    private String where;

    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public long getRowStartIndex() {
        return rowStartIndex;
    }

    public void setRowStartIndex(long rowStartIndex) {
        this.rowStartIndex = rowStartIndex;
    }

    public long getRowEndIndex() {
        return rowEndIndex;
    }

    public void setRowEndIndex(long rowEndIndex) {
        this.rowEndIndex = rowEndIndex;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getPartitionRangeName() {
        return partitionRangeName;
    }

    public void setPartitionRangeName(String partitionRangeName) {
        this.partitionRangeName = partitionRangeName;
    }

    public String getColumnNames() {
        return columnNames;
    }

    public String getWhere() {
        return where;
    }

    public void setColumnNames(String columnNames) {
        this.columnNames = columnNames;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }

    public String toSimpleString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("taskId", taskId)
                .append("tableName", tableName)
                .append("rangeName", partitionRangeName)
                .append("startIndex", getRowStartIndex())
                .append("endIndex", getRowEndIndex())
                .append("columns", getColumnNames())
                .append("where", getWhere())
                .toString();
    }


}
