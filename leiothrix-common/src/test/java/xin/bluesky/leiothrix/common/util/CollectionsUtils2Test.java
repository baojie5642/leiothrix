package xin.bluesky.leiothrix.common.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author 张轲
 */
public class CollectionsUtils2Test {

    /**
     * **************************测试集合是否为空****************************
     */
    @Test
    public void should_return_true_if_collection_is_null() throws Exception {
        // given
        List input = null;

        // when
        boolean result = CollectionsUtils2.isEmpty(input);

        // then
        assertThat(result, is(true));
    }

    @Test
    public void should_return_true_if_collection_size_is_zero() throws Exception {
        // given
        List input = new ArrayList();

        // when
        boolean result = CollectionsUtils2.isEmpty(input);

        // then
        assertThat(result, is(true));
    }

    @Test
    public void should_return_false_if_collection_is_not_empty() throws Exception {
        // given
        List input = ImmutableList.of("name", "good");

        // when
        boolean result = CollectionsUtils2.isEmpty(input);

        // then
        assertThat(result, is(false));
    }

    /**
     * **************************测试Map是否为空****************************
     */
    @Test
    public void should_return_true_if_map_is_null() throws Exception {
        // given
        Map input = null;

        // when
        boolean result = CollectionsUtils2.isEmpty(input);

        // then
        assertThat(result, is(true));
    }

    @Test
    public void should_return_true_if_map_size_is_zero() throws Exception {
        // given
        Map input = new HashMap();

        // when
        boolean result = CollectionsUtils2.isEmpty(input);

        // then
        assertThat(result, is(true));
    }

    @Test
    public void should_return_false_if_map_is_not_empty() throws Exception {
        // given
        Map input = ImmutableMap.of("name", "zhange", "age", 20);

        // when
        boolean result = CollectionsUtils2.isEmpty(input);

        // then
        assertThat(result, is(false));
    }

    /**
     * **************************测试集合转化为string****************************
     */
    @Test
    public void should_correct_to_string_from_collection() {
        // given
        List input = ImmutableList.of("name", "zhangke", "age", 20);

        // when
        String result = CollectionsUtils2.toString(input);

        // then
        assertThat(result, is("name,zhangke,age,20"));
    }

    /**
     * **************************测试Map转化为string****************************
     */
    @Test
    public void should_correct_to_string_from_map() {
        // given
        Map input = ImmutableMap.of("name", "zhangke", "age", 20);

        // when
        String result = CollectionsUtils2.toString(input);

        // then
        assertThat(result, is("name=zhangke,age=20"));
    }

    /**
     * **************************测试数组转化为string****************************
     */
    @Test
    public void should_correct_to_string_from_array() {
        // given
        String[] input = new String[]{"name", "zhangke", "age", "20"};

        // when
        String result = CollectionsUtils2.toString(input);

        // then
        assertThat(result, is("name,zhangke,age,20"));
    }
}