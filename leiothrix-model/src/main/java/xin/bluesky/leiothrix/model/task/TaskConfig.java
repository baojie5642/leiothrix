package xin.bluesky.leiothrix.model.task;

import org.apache.commons.lang3.builder.ToStringBuilder;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.builder.ToStringStyle.JSON_STYLE;

/**
 * @author 张轲
 *
 */
public class TaskConfig {

    private List<DatabaseInfo> databaseInfoList = new ArrayList();

    private List<TableQueryDefinition> tableList = new ArrayList();

    private String rangeAllocator;

    public TaskConfig() {
    }

    public List<DatabaseInfo> getDatabaseInfoList() {
        return databaseInfoList;
    }

    public void setDatabaseInfoList(List<DatabaseInfo> databaseInfoList) {
        this.databaseInfoList = databaseInfoList;
    }

    public List<TableQueryDefinition> getTableList() {
        return tableList;
    }

    public List<String> getTalbeNameList() {
        if (tableList == null) {
            return new ArrayList();
        }
        List<String> names = new ArrayList<>();
        for (TableQueryDefinition definition : tableList) {
            names.add(definition.getName());
        }
        return names;
    }

    public void setTableList(List<TableQueryDefinition> tableList) {
        this.tableList = tableList;
    }

    public String getRangeAllocator() {
        return rangeAllocator;
    }

    public void setRangeAllocator(String rangeAllocator) {
        this.rangeAllocator = rangeAllocator;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, JSON_STYLE);
    }
}
