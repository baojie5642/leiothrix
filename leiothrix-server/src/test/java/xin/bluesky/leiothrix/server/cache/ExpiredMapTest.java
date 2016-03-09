package xin.bluesky.leiothrix.server.cache;

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

/**
 * @author 张轲
 */
public class ExpiredMapTest {

    @Test
    public void should_operate_correct() {
        ExpiredMap<String, String> map = new ExpiredMap<>(100);
        map.put("my", "张轲");
        map.put("you", "unknow");

        assertThat(map.get("my"), is("张轲"));
        map.put("you", "another");
        assertThat(map.get("you"), is("another"));

        map.remove("you");
        assertThat(map.get("you"), nullValue());
        assertThat(map.containsKey("you"), is(false));

        String old = map.putIfAbsent("my", "newname");
        assertThat(old, is("张轲"));
        old = map.putIfAbsent("notexist", "newname");
        assertThat(old, nullValue());
    }

    @Test
    @Ignore(value = "这个测试比较耗费时间,且不怎么会改,测试通过后就ignore了,如有变更再打开验证")
    public void should_expire_correct() throws Exception {
        ExpiredMap<String, String> a = new ExpiredMap<>(5);
        a.put("key", "value");
        ExpiredMap<String, Integer> b = new ExpiredMap<>(15);
        b.put("keyb", 1001);

        Thread.sleep(12 * 1000);

        assertThat(a.isEmpty(), is(true));
        assertThat(b.isEmpty(), is(false));

        Thread.sleep(21 * 1000);
        assertThat(b.isEmpty(), is(true));
    }
}