package xin.bluesky.leiothrix.server.tablemeta;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;
import xin.bluesky.leiothrix.model.db.DialectType;
import xin.bluesky.leiothrix.model.task.TaskStaticInfo;
import xin.bluesky.leiothrix.server.conf.ConfigureException;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author 张轲
 */
public abstract class DatabaseSchemaLoader {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseSchemaLoader.class);

    private static Map<String, DatabaseSchemaLoader> map = ImmutableMap.of(DialectType.MYSQL, new MySQLSchemaLoader());

    private static ThreadPoolExecutor executorService = new ThreadPoolExecutor(5, 20, 60, SECONDS, new LinkedBlockingDeque<>(),
            new ThreadFactoryBuilder().setNameFormat("database-split-%d").build(), new CallerRunsPolicy());

    public static void loadDatabaseSchema(String taskId) {
        TaskStaticInfo taskStaticInfo = TaskStorage.getTaskStaticInfo(taskId);
        DatabaseInfo anyDatabaseInfo = taskStaticInfo.getTaskConfig().getDatabaseInfoList().get(0);
        DatabaseSchemaLoader loader = map.get(anyDatabaseInfo.getDialect());
        if (loader == null) {
            throw new ConfigureException(String.format("不支持%s数据库", anyDatabaseInfo.getDialect()));
        }

        loader.doLoad(taskId, anyDatabaseInfo, taskStaticInfo.getTaskConfig().getTableNameList());
        logger.info("加载数据库Schema完成");
    }

    protected void submit(RangeSplitter splitter) {
        executorService.submit(splitter);
    }

    protected abstract void doLoad(String taskId, DatabaseInfo databaseInfo, List<String> tableList);
}
