package xin.bluesky.leiothrix.server.action.allocate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.server.bean.status.RangeStatus;
import xin.bluesky.leiothrix.server.bean.status.TableStatus;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;

import java.util.List;

/**
 * @author 张轲
 */
public class RangeScanner {

    private static final Logger logger = LoggerFactory.getLogger(RangeScanner.class);

    private String taskId;

    private String tableName;

    public RangeScanner(String taskId, String tableName) {
        this.taskId = taskId;
        this.tableName = tableName;
    }

    /**
     * 扫描某个表的range,以找到尚未被分配的range.如果没有找到,则返回null.在扫描过程中,如果所有range都已结束,则更新表状态为结束
     *
     * @return {@link RangeScanResult} object
     */
    public RangeScanResult scan() {
        RangeScanResult result = new RangeScanResult();

        List<String> rangeNames = RangeStorage.getAllRangesByTableName(taskId, tableName);
        boolean hasUnfinishedRange = false;

        for (String rangeName : rangeNames) {
            RangeStatus rangeStatus = RangeStorage.getRangeStatus(taskId, tableName, rangeName);
            switch (rangeStatus) {
                case UNALLOCATED:
                    hasUnfinishedRange = true;
                    result.addUnallocatedRangeName(rangeName);
                    break;
                case PRE_ALLOCATE:
                case PROCESSING:
                    hasUnfinishedRange = true;
                    break;
                case FINISHED:
                    break;
                default:
                    break;
            }
        }


        // 如果全部结束了,则更新table的状态
        if (!hasUnfinishedRange) {
            result.setIsTableFinished(true);
            TableStorage.setStatus(taskId, tableName, TableStatus.FINISHED);
            logger.info("任务[taskId={}]中的表{}被执行完毕", taskId, tableName);
        }

        return result;
    }
}
