package xin.bluesky.leiothrix.server.support;

import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;


/**
 * @author 张轲
 * @date 16/2/5
 */
public class RangesCreator {

    private String taskId;

    public RangesCreator(String taskId) {
        this.taskId = taskId;
    }

    public void createRanges() {
        TableMeta table1Meta = new TableMeta("table-1", "id");
        TableMeta table2Meta = new TableMeta("table-2", "id");
        TableMeta table3Meta = new TableMeta("table-3", "id");

        TableStorage.createTable(taskId, table1Meta);
        TableStorage.createTable(taskId, table2Meta);
        TableStorage.createTable(taskId, table3Meta);
        TableStorage.setStatus(taskId, table3Meta.getTableName(), TableStatus.FINISHED);

        RangeStorage.createRange(taskId, "table-1", "0-100");
        RangeStorage.createRange(taskId, "table-1", "101-200");
        RangeStorage.createRange(taskId, "table-2", "0-1000");
        RangeStorage.createRange(taskId, "table-2", "1001-2000");
    }

}
