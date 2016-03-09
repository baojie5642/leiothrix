package xin.bluesky.leiothrix.model.task;

/**
 * @author 张轲
 * @date 16/3/8
 */
public class TableQueryDefinition {

    private String name;

    private String columns;

    private String where;

    public TableQueryDefinition() {
    }

    public TableQueryDefinition(String name, String columns, String where) {
        this.name = name;
        this.columns = columns;
        this.where = where;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }
}
