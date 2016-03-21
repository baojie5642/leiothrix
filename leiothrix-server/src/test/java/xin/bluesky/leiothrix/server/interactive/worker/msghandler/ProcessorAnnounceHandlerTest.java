package xin.bluesky.leiothrix.server.interactive.worker.msghandler;

import com.alibaba.fastjson.JSON;
import org.junit.Test;
import xin.bluesky.leiothrix.model.msg.WorkerMessage;
import xin.bluesky.leiothrix.model.stat.ProcessorInfo;
import xin.bluesky.leiothrix.server.storage.WorkerStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static xin.bluesky.leiothrix.model.msg.WorkerMessageType.PROCESSOR_ANNOUNCE;

/**
 * @author 张轲
 */
public class ProcessorAnnounceHandlerTest extends StorageContainerDependency {

    @Test
    public void should_decrease_worker_number() throws Exception {
        // given
        final String workerIp = "192.168.5.233";
        WorkerStorage.setWorkersNumber(workerIp, 2);

        ProcessorInfo processor = new ProcessorInfo("taskId", workerIp, "1001");
        processor.setStep(ProcessorInfo.STEP_EXIT);
        processor.setSuccess(true);
        WorkerMessage msg = new WorkerMessage(PROCESSOR_ANNOUNCE, JSON.toJSONString(processor), workerIp);

        // when
        ProcessorAnnounceHandler handler = new ProcessorAnnounceHandler();
        handler.handle(null, msg);

        // then
        assertThat(WorkerStorage.getWorkersNumber(workerIp), is(1));
    }


}