package xin.bluesky.leiothrix.server.tablemeta;

/**
 * @author 张轲
 */
public class TableMeta {
    private String tableName;

    private String primaryKey;

    public TableMeta() {
    }

    public TableMeta(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
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
}
