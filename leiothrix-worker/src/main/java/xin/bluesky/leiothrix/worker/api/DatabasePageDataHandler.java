package xin.bluesky.leiothrix.worker.api;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

/**
 * 提供给application的数据处理API.
 *
 * <p>server上的每个worker,按照server划分给其的任务片,再分页执行.在该页执行完后,向server汇报进度.
 * 而该API中的{@link #handle(String, List)}中的dataList就是一页的数据.</p>
 *
 * <p>为什么提供针对页的接口,而不是数据行的接口? 因为如worker在处理该页数据时die了,其他worker会接管,
 * 这会造成数据被重复消费.但如果application的数据处理逻辑是可以自行保证事务的(比如update等数据库操作),
 * 那么worker die,数据处理的结果也会回滚,从而其他worker接管时,不会有重复消费的情况发生.
 * 综上所述,提供按页的接口,是为了将是否能够避免重复消费的能力,放到application端,由其自行控制.
 * 当然,从理论上来说,从该页数据消费成功,到上报执行进度到server,也存在时间间隔,但这个窗口非常小,
 * 极大地减少了重复消费的可能性.
 * </p>
 *
 * @author 张轲
 */
public interface DatabasePageDataHandler {

    void handle(String tableName, List<JSONObject> dataList) throws Exception;

    void exceptionCaught(String tableName, List<JSONObject> dataList, Exception e);
}
