package xin.bluesky.leiothrix.model.msg;

import java.io.Serializable;

/**
 * @author 张轲
 *         worker.processor.threadnum.factor
 */
public class WorkerMessageType implements Serializable {

    public static final String ACQUIRE_TASK = "acquireTask";

    public static final String FINISHED_TASK = "finishedTask";

    public static final String EXECUTE_PROGRESS_REPORT = "workerProgressReport";

    public static final String PROCESSOR_ANNOUNCE = "processorAnnounce";

    public static final String GIVE_BACK_PARTITION_TASK = "giveBackPartitionTask";

    public static final String PING = "ping";
}
