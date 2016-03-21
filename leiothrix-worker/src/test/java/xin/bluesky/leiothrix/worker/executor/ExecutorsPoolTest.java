package xin.bluesky.leiothrix.worker.executor;

import org.junit.Test;
import xin.bluesky.leiothrix.worker.conf.Settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * @author 张轲
 */
public class ExecutorsPoolTest {

    @Test
    public void should_cal_executor_numbers_correct() throws Exception {
        Settings.setThreadNumFactor(3);

        ExecutorsPool executorsPool = new ExecutorsPool();
        int value = executorsPool.calExecutorsNumbers();

        //由于依赖于运行机器的物理内存,和启动进程时的堆大小,所以无法准确测试
        assertThat(value, greaterThan(0));
    }
}