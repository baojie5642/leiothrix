package xin.bluesky.leiothrix.worker.action;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * @author 张轲
 */
public class ProcessorAnnouncerTest {

    @Test
    public void should_get_processor_id() throws Exception {
        assertThat(ProcessorAnnouncer.getProcessInfo().getProcessorId(), notNullValue());
    }

}