package xin.bluesky.leiothrix.examples.print;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.worker.api.DatabasePageDataHandler;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.worker.conf.WorkerConfiguration;
import xin.bluesky.leiothrix.worker.WorkerProcessor;

import java.util.List;

/**
 * @author 张轲
 */
public class PrintDataExample {

    private static final Logger logger = LoggerFactory.getLogger(PrintDataExample.class);

    public static void main(String[] args) throws Exception {
        logger.info("开始执行{}", PrintDataExample.class.getSimpleName());

        WorkerConfiguration configuration=new WorkerConfiguration();
        configuration.setDatabasePageDataHandler(new DatabasePageDataHandler() {
            @Override
            public void handle(String tableName, String primaryKey, List<JSONObject> dataList) throws Exception {
                dataList.forEach(data -> {
                    data.entrySet().forEach(entry -> {
                        logger.info(entry.getKey() + ":" + entry.getValue());
                    });
                });
            }

            @Override
            public void exceptionCaught(String tableName, List<JSONObject> dataList, Exception e) {
                logger.error("处理表[{}]数据时发生异常,数据为:{},异常为:{}",
                        tableName, CollectionsUtils2.toString(dataList), ExceptionUtils.getStackTrace(e));
            }
        });

        WorkerProcessor bootStrap = new WorkerProcessor(configuration);

        bootStrap.start();
    }
}
