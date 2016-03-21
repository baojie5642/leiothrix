package xin.bluesky.leiothrix.server.tablemeta;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;
import xin.bluesky.leiothrix.common.jdbc.ParallelJdbcExecutor;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;
import xin.bluesky.leiothrix.server.Constant;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.storage.RangeStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author 张轲
 */
public class RangeSplitter implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RangeSplitter.class);

    private String taskId;

    private DatabaseInfo databaseInfo;

    private TableMeta tableMeta;

    private CountDownLatch countDownLatch;

    public RangeSplitter(String taskId, DatabaseInfo databaseInfo, TableMeta tableMeta, CountDownLatch countDownLatch) {
        this.taskId = taskId;
        this.databaseInfo = databaseInfo;
        this.tableMeta = tableMeta;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);

            List<JSONObject> result = jdbcTemplate.query("select max(" + tableMeta.getPrimaryKey() + ") as maxid, min(" + tableMeta.getPrimaryKey() + ") as minid from " + tableMeta.getTableName());
            Long maxId = result.get(0).getLong("maxid");
            Long minId = result.get(0).getLong("minid");
            if (maxId == null) {//表明该表没有记录,不分配range
                return;
            }

            List<String> rangeNameList = splitIdRange(minId, maxId);
            logger.info("查得表[tableName={}]的数据范围,minId={},maxId={}", tableMeta.getTableName(), minId, maxId);

            ParallelJdbcExecutor parallel = new ParallelJdbcExecutor(jdbcTemplate);
            rangeNameList.forEach(rangeName -> {
                parallel.put(rangeName, getQueryRangeRecordNumSQL(rangeName));
            });

            Map<String, List<JSONObject>> numResult = parallel.queryForMap();
            numResult.entrySet().forEach(entry -> {
                String rangeName = entry.getKey();
                JSONObject r = entry.getValue().get(0);
                int recordNum = r.getInteger("recordNum");
                RangeStorage.createRange(taskId, tableMeta.getTableName(), rangeName, recordNum);
            });

            logger.debug("加载表[tableName={},primaryKey={}]分片信息:{}", tableMeta.getTableName(), tableMeta.getPrimaryKey(), CollectionsUtils2.toString(rangeNameList));
        } finally {
            countDownLatch.countDown();
        }
    }

    private String getQueryRangeRecordNumSQL(String rangeName) {
        String[] r = rangeName.split(Constant.RANGE_SEPARATOR);
        return StringUtils2.append(
                "select count(1) recordNum from ", tableMeta.getTableName(),
                " where ", tableMeta.getPrimaryKey(), ">=", r[0], " and ", tableMeta.getPrimaryKey(), "<=", r[1]).toString();
    }

    private List<String> splitIdRange(long minId, long maxId) {
        List<String> result = new ArrayList<>();

        int tableRange = Integer.parseInt(ServerConfigure.get("table.range"));
        long currentId = minId;
        while (true) {
            long nextId = currentId + tableRange - 1;
            if (nextId < maxId) {
                result.add(currentId + Constant.RANGE_SEPARATOR + nextId);
                currentId = nextId + 1;
            } else {
                result.add(currentId + Constant.RANGE_SEPARATOR + maxId);
                break;
            }
        }
        return result;
    }
}
