package xin.bluesky.leiothrix.model.db;

/**
 * @author 张轲
 */
public class Column {
    private ColumnType type;

    private Object data;

    public Column(ColumnType type, Object data) {
        this.type = type;
        this.data = data;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public static enum ColumnType {
        STRING,NUMBER,DATE
    }
}
