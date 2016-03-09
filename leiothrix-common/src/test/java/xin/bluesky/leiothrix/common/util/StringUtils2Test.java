package xin.bluesky.leiothrix.common.util;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author 张轲
 * @date 16/2/3
 */
public class StringUtils2Test {

    /**
     * **************************测试将日期转化为时间戳****************************
     */
    @Test
    public void should_append_correct() {
        // when
        String result = StringUtils2.append("a", "b", "d", 1, 9, "2");

        //then
        assertThat(result, is("abd192"));
    }

}