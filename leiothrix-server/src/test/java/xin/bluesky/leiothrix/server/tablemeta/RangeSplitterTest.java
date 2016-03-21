package xin.bluesky.leiothrix.server.tablemeta;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;

import static org.junit.Assert.*;

/**
 * @author 张轲
 */
@RunWith(MockitoJUnitRunner.class)
public class RangeSplitterTest {

    @InjectMocks
    private RangeSplitter rangeSplitter;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    public void should_run_correct(){

    }
}