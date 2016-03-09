package xin.bluesky.leiothrix.server.tablemeta;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author 张轲
 */
public class MySQLSchemaLoader extends DatabaseSchemaLoader {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSchemaLoader.class);

    @Override
    protected void doLoad(String taskId, DatabaseInfo databaseInfo, List<String> tableList) {

        List<TableMeta> tableMetaList = getTablesMeta(databaseInfo, tableList);

        final CountDownLatch countDownLatch = new CountDownLatch(tableMetaList.size());
        tableMetaList.forEach(tableMeta -> {
            TableStorage.createTable(taskId, tableMeta);
            submit(new RangeSplitter(taskId, databaseInfo, tableMeta, countDownLatch));
        });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("在给任务[taskId={}]分片时线程被中断", taskId);
        }
    }

    private List<TableMeta> getTablesMeta(DatabaseInfo databaseInfo, List<String> tableList) {
        //不支持组合主键
        String sql = StringUtils2.append("select table_name,column_name from INFORMATION_SCHEMA.COLUMNS",
                " where column_key='PRI' and TABLE_SCHEMA=",
                "'", databaseInfo.getSchema().toLowerCase(), "' ");

        if (!CollectionsUtils2.isEmpty(tableList)) {
            StringBuffer tableCondition = new StringBuffer();

            tableList.forEach(tableName -> {
                tableCondition.append("'").append(tableName).append("',");
            });

            tableCondition.deleteCharAt(tableCondition.length() - 1);

            sql = StringUtils2.append(sql, " and table_name in (", tableCondition, ")");
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        List<JSONObject> result = jdbcTemplate.query(sql);
        List<TableMeta> tableMetaList = new ArrayList();
        result.forEach((meta) -> {
            String tableName = meta.getString("table_name".toUpperCase());
            String primaryKey = meta.getString("column_name".toUpperCase());
            tableMetaList.add(new TableMeta(tableName, primaryKey));
        });

        return tableMetaList;
    }

}
