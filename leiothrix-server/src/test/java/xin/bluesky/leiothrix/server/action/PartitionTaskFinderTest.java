package xin.bluesky.leiothrix.server.action;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.action.allocate.SequencePartitionAllocator;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;
import xin.bluesky.leiothrix.model.db.DialectType;
import xin.bluesky.leiothrix.model.task.TaskConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

/**
 * @author 张轲
 */
public class PartitionTaskFinderTest extends StorageContainerDependency {

    private static final Logger logger = LoggerFactory.getLogger(PartitionTaskFinderTest.class);

    /**
     * **************************测试能够在集合中均匀地获得数据库****************************
     */
    @Test
    public void should_choose_database_evenly() throws Exception {
        // given
        DatabaseInfo db1 = new DatabaseInfo(DialectType.MYSQL, "localhost", 3306, "test1", "user", "password");
        DatabaseInfo db2 = new DatabaseInfo(DialectType.MYSQL, "localhost", 3310, "test2", "user", "password");

        //--- 生成config文件
        TaskConfig taskConfig = new TaskConfig();
        taskConfig.setDatabaseInfoList(ImmutableList.of(db1, db2));

        //--- 将config文件的路径存放到zookeeper上的任务下面的配置节点上
        String taskId = UUID.randomUUID().toString();
        paddingTaskStaticInfo(taskId, taskConfig);

        // when
        Map<String, Integer> countMap = new HashMap();
        PartitionTaskFinder finder = new PartitionTaskFinder(taskId, new SequencePartitionAllocator());
        // 执行方法N多次,验证结果近乎平均分配
        final int totalCount = 500;
        for (int i = 0; i < totalCount; i++) {
            DatabaseInfo db = finder.chooseSuitableDatabase(taskId);
            String key = db.toString();
            if (countMap.containsKey(key)) {
                countMap.put(key, countMap.get(key).intValue() + 1);
            } else {
                countMap.put(key, 1);
            }
        }

        // then
        Integer db1Count = countMap.get(db1.toString());
        Integer db2Count = countMap.get(db2.toString());
        logger.info("totalCount={},db1Count={},db2Count={}", totalCount, db1Count, db2Count);
        assertThat(Math.abs(db1Count - db2Count), lessThan(50));
    }

    private void paddingTaskStaticInfo(String taskId, TaskConfig taskConfig) throws IOException {
        TaskStorage.setJarPath(taskId, "/jar");
        TaskStorage.setMainClass(taskId, "MainClass");
        TaskStorage.setConfig(taskId, JSON.toJSONString(taskConfig));
    }
}