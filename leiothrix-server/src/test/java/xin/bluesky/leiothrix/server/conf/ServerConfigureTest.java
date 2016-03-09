package xin.bluesky.leiothrix.server.conf;

import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author 张轲
 * @date 16/2/3
 */
public class ServerConfigureTest {

    private void createConfig(String key, String value) throws Exception {
        Properties config = new Properties();
        config.setProperty(key, value);
        ServerConfigure.setProperties(config);
    }

    @Test
    public void should_get_value() throws Exception {
        // given
        createConfig("server", "localhost");

        // when
        assertThat(ServerConfigure.get("server"), is("localhost"));
    }

    @Test(expected = ConfigureException.class)
    public void should_throw_exception_if_key_not_exist() {
        ServerConfigure.get("nokey");
    }

    @Test
    public void should_get_integer_value() throws Exception {
        // given
        createConfig("server.port.client", "12801");

        // when
        assertThat(ServerConfigure.get("server.port.client", Integer.class), is(12801));
    }
}