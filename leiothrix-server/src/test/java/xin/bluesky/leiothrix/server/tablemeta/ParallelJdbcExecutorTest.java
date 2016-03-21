package xin.bluesky.leiothrix.server.tablemeta;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import xin.bluesky.leiothrix.common.jdbc.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

/**
 * @author 张轲
 */
@RunWith(MockitoJUnitRunner.class)
public class ParallelJdbcExecutorTest {

    @InjectMocks
    private ParallelJdbcExecutor executor;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    public void should_query_for_map_correct() throws Exception {
        // given
        String sql1 = "select count(1) recordnum from table1";
        String sql2 = "select count(2) recordnum from table2";

        // when
        when(jdbcTemplate.query(sql1)).thenReturn(ImmutableList.of(
                new JSONObject(
                        ImmutableMap.of(
                                "recordnum", 2
                        )
                )
        ));
        when(jdbcTemplate.query(sql2)).thenReturn(ImmutableList.of(
                new JSONObject(
                        ImmutableMap.of(
                                "recordnum", 10
                        )
                )
        ));

        executor.put("key1", sql1);
        executor.put("key2", sql2);
        Map<String, List<JSONObject>> result = executor.queryForMap();

        // then
        assertThat(result.size(), is(2));
        assertThat(result.get("key1").get(0).getInteger("recordnum"), is(2));
        assertThat(result.get("key2").get(0).getInteger("recordnum"), is(10));
    }

    @Test
    public void should_query_for_list_correct() throws Exception {
        // given
        String sql1 = "select count(1) recordnum from table1";
        String sql2 = "select count(2) recordnum from table2";

        // when
        when(jdbcTemplate.query(sql1)).thenReturn(ImmutableList.of(
                new JSONObject(
                        ImmutableMap.of(
                                "recordnum", 2
                        )
                )
        ));
        when(jdbcTemplate.query(sql2)).thenReturn(ImmutableList.of(
                new JSONObject(
                        ImmutableMap.of(
                                "recordnum", 10
                        )
                )
        ));

        executor.add(sql1);
        executor.add(sql2);
        List<JSONObject> result = executor.queryForList();

        // then
        assertThat(result.size(), is(2));
        assertThat(result.get(0).getInteger("recordnum"), is(10));
        assertThat(result.get(1).getInteger("recordnum"), is(2));
    }
}