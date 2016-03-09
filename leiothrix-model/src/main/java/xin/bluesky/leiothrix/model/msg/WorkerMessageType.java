package xin.bluesky.leiothrix.model.msg;

import java.io.Serializable;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class WorkerMessageType implements Serializable {

    public static final String ACQUIRE_TASK = "acquireTask";

    public static final String FINISHED_TASK = "finishedTask";

    public static final String WORKER_PROGRESS_REPORT = "workerProgressReport";

    public static final String WORKER_NUM_INCR = "workerNumberIncr";

    public static final String WORKER_NUM_DECR = "workerNumberDecr";

    public static final String PING = "ping";
}
