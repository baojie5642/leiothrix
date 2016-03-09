package xin.bluesky.leiothrix.common.util;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author 张轲
 * @date 16/2/3
 */
public class DateUtils2Test {

    /**
     * **************************测试格式化时间戳为长字符串****************************
     */
    @Test
    public void should_return_correct_string_when_format_full() {
        // given
        long time = 1454469569718l;

        // when
        String result = DateUtils2.formatFull(time);

        // then
        assertThat(result, is("2016-02-03 11:19:29"));
    }

    /**
     * **************************测试将日期转化为时间戳****************************
     */
    @Test
    public void should_return_correct_timestamp_string() throws Exception {
        // given
        Date date = new SimpleDateFormat(DateUtils2.FORMAT_FULL).parse("2016-02-03 11:19:29");

        // when
        String result = DateUtils2.toTime(date);

        // then
        assertThat(result, is("1454469569000"));
    }

    @Test
    public void should_return_blank_string_if_date_is_null() throws Exception {
        // given
        Date date = null;

        // when
        String result = DateUtils2.toTime(date);

        // then
        assertThat(result, is(""));
    }

}