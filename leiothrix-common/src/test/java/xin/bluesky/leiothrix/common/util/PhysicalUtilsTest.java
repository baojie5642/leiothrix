package xin.bluesky.leiothrix.common.util;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_HALF_DOWN;

/**
 * @author 张轲
 */
@Ignore(value = "取决于回收策略,并且执行时间较长")
public class PhysicalUtilsTest {

    private static final Logger logger = LoggerFactory.getLogger(PhysicalUtilsTest.class);

    @Test
    public void should_print_old_gc_times() {
        PhysicalUtils.getOldGcTimes();
    }

    @Test
    public void should_print_cpu_load() throws Exception {
        DeadLoop t1 = new DeadLoop();
        DeadLoop t2 = new DeadLoop();
        t1.start();
        t2.start();


        int count = 0;
        while (true) {
            System.out.println(PhysicalUtils.getCpuLoad().multiply(new BigDecimal(100)).setScale(2, ROUND_HALF_DOWN) + "%");
            Thread.sleep(1000);
            count++;
            if (count == 15) {
                t1.stopLoop();
            }
            if (count == 30) {
                t2.stopLoop();
            }
            if (count == 40) {
                break;
            }
        }
    }

    private class DeadLoop extends Thread {

        volatile boolean stop = false;

        @Override
        public void run() {
            while (!isStopped()) {

            }
            logger.info("跳出死循环");
        }

        public void stopLoop() {
            stop = true;
        }

        public boolean isStopped() {
            return stop;
        }
    }

}