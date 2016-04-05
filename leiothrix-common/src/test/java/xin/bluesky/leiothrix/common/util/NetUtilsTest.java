package xin.bluesky.leiothrix.common.util;

import org.junit.Ignore;
import org.junit.Test;
import xin.bluesky.leiothrix.common.net.NetUtils;

import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author 张轲
 * worker.processor.threadnum.factor
 */
public class NetUtilsTest {
    /**
     * **********************测试获得本地IP*************************************************
     */
    @Test
    public void should_get_local_ip() throws Exception {
        assertThat(NetUtils.getLocalIp(), notNullValue());
    }

    @Test
    public void test(){
        UUID uuid=UUID.randomUUID();
        System.out.println(uuid.toString());
        System.out.println(uuid.getLeastSignificantBits());
    }

    /**
     * **********************测试ping功能*************************************************
     */
    @Test
    public void should_ping_success() throws Exception {
        // given
        String remoteIp = "127.0.0.1";

        // when
        boolean result = NetUtils.pingSuccess(remoteIp);

        // then
        assertThat(result, is(true));
    }

    @Test
    public void should_ping_fail() throws Exception {
        // given
        String remoteIp = "128.0.0.0";

        // when
        boolean result = NetUtils.pingSuccess(remoteIp);

        // then
        assertThat(result, is(false));
    }

    /**
     * **********************测试ssh功能*************************************************
     */
    @Ignore("ssh的测试需要根据实际情况来调整测试数据,所以暂ignore掉")
    @Test
    public void should_ssh_success() throws Exception {
        // given
        String remoteIp = "192.168.5.103";
        String username = "root";

        // when
        boolean result = NetUtils.sshSuccess(remoteIp, username);

        // then
        assertThat(result, is(true));
    }

    @Ignore
    @Test
    public void should_ssh_fail() throws Exception {
        // given
        String remoteIp = "192.168.5.103";
        String username = "notAllowedUser";

        // when
        boolean result = NetUtils.sshSuccess(remoteIp, username);

        // then
        assertThat(result, is(false));
    }
}