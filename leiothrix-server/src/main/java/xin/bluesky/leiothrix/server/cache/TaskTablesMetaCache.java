package xin.bluesky.leiothrix.server.cache;

import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.tablemeta.TableMeta;

/**
 * @author 张轲
 * @date 16/3/3
 */
public class TaskTablesMetaCache {

    private static ExpiredMap<String, TableMeta> map = new ExpiredMap<>(5 * 60);

    public static TableMeta get(String taskId, String tableName) {
        String key = getKey(taskId, tableName);
        return map.get(key);
    }

    private static String getKey(String taskId, String tableName) {
        return StringUtils2.append(taskId, ":", tableName);
    }

    public static void put(String taskId, TableMeta tableMeta) {
        String key = getKey(taskId, tableMeta.getTableName());
        map.put(key, tableMeta);
    }

    private TaskTablesMetaCache() {
    }
}
