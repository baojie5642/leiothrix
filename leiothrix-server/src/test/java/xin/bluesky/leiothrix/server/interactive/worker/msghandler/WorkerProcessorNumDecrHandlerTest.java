package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import org.junit.Test;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.WORKER_NUM_DECR;

/**
 * @author 张轲
 * @date 16/2/25
 */
public class WorkerProcessorNumDecrHandlerTest extends StorageContainerDependency {

    @Test
    public void should_decrease_worker_number() throws Exception {
        // given
        final String workerIp = "192.168.5.233";
        WorkerStorage.setWorkersNumber(workerIp, 2);
        WorkerMessage msg = new WorkerMessage(WORKER_NUM_DECR, "", workerIp);

        // when
        WorkerProcessorNumDecrHandler handler = new WorkerProcessorNumDecrHandler();
        handler.handle(null, msg);

        // then
        assertThat(WorkerStorage.getWorkersNumber(workerIp), is(1));
    }


}