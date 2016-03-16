package xin.bluesky.leiothrix.common.jdbc;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;
import xin.bluesky.leiothrix.model.db.DialectType;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.FluentIterable.from;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author 张轲
 */
public class JdbcTemplateTest {

    private DatabaseInfo databaseInfo;

    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.load(JdbcTemplateTest.class.getResourceAsStream("/mysql.properties"));
        databaseInfo = new DatabaseInfo();
        databaseInfo.setDialect(DialectType.MYSQL);
        databaseInfo.setIp(properties.getProperty("db.ip"));
        databaseInfo.setPort(Integer.parseInt(properties.getProperty("db.port")));
        databaseInfo.setSchema(properties.getProperty("db.schema"));
        databaseInfo.setUserName(properties.getProperty("db.user"));
        databaseInfo.setPassword(properties.getProperty("db.password"));
    }

    /**
     * **************************测试根据Statement查询****************************
     */
    @Test
    public void should_query_correct_by_statement() throws Exception {
        // given
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        String sql = "select * from column_type_test where id=10009812";

        // when
        JSONObject result = jdbcTemplate.query(sql).get(0);

        // then
        assertThat(result.getString("Id"), is("10009812"));
        assertThat(result.getString("SmallIntColumn"), is("1"));
        assertThat(result.getString("TinyIntColumn"), is("1"));
        assertThat(result.getString("BigIntColumn"), is("200999121231212"));
        assertThat(result.getString("FloatColumn"), is("100.2"));
        assertThat(result.getString("DoubleColumn"), is("12.0123"));
        assertThat(result.getString("BitColumn"), is("true"));
        assertThat(result.getString("BooleanColumn"), is("true"));
        assertThat(result.getString("CharColumn"), is("a"));
        assertThat(result.getString("VarcharColumn"), is("good boy"));
        assertThat(result.getString("DateColumn"), is("2016-02-03"));
        assertThat(result.getString("DateTimeColumn"), is("2016-02-03 11:50:42"));
        assertThat(result.getString("TimeStampColumn"), is("2016-02-03 11:51:01"));
        assertThat(result.getString("TimeColumn"), is("11:50:47"));
    }

    @Test
    public void should_query_correct_by_statement_if_value_is_null() throws Exception {
        // given
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        String sql = "select * from column_type_test where id=10009822";

        // when
        JSONObject result = jdbcTemplate.query(sql).get(0);

        // then
        assertThat(result.getString("Id"), is("10009822"));
        assertThat(result.getString("SmallIntColumn"), nullValue());
        assertThat(result.getString("TinyIntColumn"), nullValue());
        assertThat(result.getString("BigIntColumn"), nullValue());
        assertThat(result.getString("FloatColumn"), nullValue());
        assertThat(result.getString("DoubleColumn"), nullValue());
        assertThat(result.getString("BitColumn"), nullValue());
        assertThat(result.getString("BooleanColumn"), nullValue());
        assertThat(result.getString("CharColumn"), nullValue());
        assertThat(result.getString("VarcharColumn"), nullValue());
        assertThat(result.getString("DateColumn"), nullValue());
        assertThat(result.getString("DateTimeColumn"), nullValue());
        assertThat(result.getString("TimeStampColumn"), nullValue());
        assertThat(result.getString("TimeColumn"), nullValue());
    }

    /**
     * **************************测试根据PreparedStatement查询****************************
     */
    @Test
    public void should_query_correct_by_preparedstatement() throws Exception {
        // given
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        String sql = "select * from column_type_test where id=?";

        // when
        JSONObject result = jdbcTemplate.query(sql, "10009812").get(0);

        // then
        assertThat(result.getString("Id"), is("10009812"));
    }

    /**
     * **************************测试增删改操作****************************
     */
    @Test
    public void should_insert_correct() throws Exception {
        // given
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        String insertSql = "insert into column_type_test(TinyIntColumn,DecimalColumn,VarcharColumn,DateColumn,DateTimeColumn)" +
                " values(?,?,?,?,?)";
        String updateSql = "update column_type_test set VarcharColumn=? where id=?";
        String deleteSql = "delete from column_type_test where id=?";

        // 测试插入
        int newId = jdbcTemplate.insert(insertSql, 1, 100.43, "myname", new Date(), new Date());
        JSONObject obj = jdbcTemplate.query("select * from column_type_test where id=?", newId).get(0);
        assertThat(obj.getString("TinyIntColumn"), is("1"));
        assertThat(obj.getDate("DateColumn"), notNullValue());

        // 测试更新
        jdbcTemplate.update(updateSql, "yourname", newId);
        JSONObject o2 = jdbcTemplate.query("select * from column_type_test where id=?", newId).get(0);
        assertThat(o2.getString("VarcharColumn"), is("yourname"));

        // 测试删除
        jdbcTemplate.delete(deleteSql, newId);
        List<JSONObject> list = jdbcTemplate.query("select * from column_type_test where id=?", newId);
        assertThat(list.size(), is(0));
    }


    /**
     * **************************测试批量操作****************************
     */
    @Test
    public void should_insert_batch_correct() throws Exception {
        // given
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        String insertSql = "insert into column_type_test(TinyIntColumn,DecimalColumn,VarcharColumn,DateColumn,DateTimeColumn)" +
                " values(?,?,?,?,?)";
        String updateSql = "update column_type_test set VarcharColumn=? where id=?";
        String deleteSql = "delete from column_type_test where id=?";

        // 测试批量插入
        List<Object[]> data = ImmutableList.of(
                new Object[]{1, 100.43, "myname", new Date(), new Date()},
                new Object[]{2, 101, "myname2", new Date(), new Date()}
        );
        List<Integer> newIds = jdbcTemplate.insertBatch(insertSql, data);
        JSONObject obj = jdbcTemplate.query("select * from column_type_test where id=?", newIds.get(0)).get(0);
        assertThat(obj.getString("TinyIntColumn"), is("1"));
        assertThat(obj.getDate("DateColumn"), notNullValue());

        // 测试批量更新
        jdbcTemplate.updateBatch(updateSql, from(newIds).transform(new Function<Integer, Object[]>() {
            @Override
            public Object[] apply(Integer newId) {
                return new Object[]{"newName", newId};
            }
        }).toList());
        List<JSONObject> list = jdbcTemplate.query("select * from column_type_test where id=? or id=?", newIds.get(0), newIds.get(1));
        assertThat(list.get(0).getString("VarcharColumn"), is("newName"));
        assertThat(list.get(1).getString("VarcharColumn"), is("newName"));

        // 测试批量删除
        jdbcTemplate.deleteBatch(deleteSql, from(newIds).transform(new Function<Integer, Object[]>() {
            @Override
            public Object[] apply(Integer newId) {
                return new Object[]{newId};
            }
        }).toList());
        list = jdbcTemplate.query("select * from column_type_test where id=? or id=?", newIds.get(0), newIds.get(1));
        assertThat(list.size(), is(0));
    }

    /**
     * 测试通过表名和JSON数据批量插入
     */
    @Test
    public void should_insert_all_column_batch_success() throws Exception {
        // given
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        String tableName = "column_type_test";
        List<JSONObject> dataList = ImmutableList.of(
                new JSONObject(
                        ImmutableMap.of("TinyIntColumn", 1, "VarcharColumn", "a", "DateColumn", new Date())
                ),
                new JSONObject(
                        ImmutableMap.of("TinyIntColumn", 2, "VarcharColumn", "b", "DateColumn", new Date())
                ),
                new JSONObject(
                        ImmutableMap.of("DateColumn", new Date(), "VarcharColumn", "b", "TinyIntColumn", 3)
                )
        );

        // when
        List<Integer> newIds = jdbcTemplate.insertAllColumnBatch(tableName, dataList);
        JSONObject obj = jdbcTemplate.query("select * from column_type_test where id=?", newIds.get(0)).get(0);
        assertThat(obj.getString("TinyIntColumn"), is("1"));
        assertThat(obj.getDate("DateColumn"), notNullValue());
    }

    /**
     * 测试执行ddl
     *
     * @throws Exception
     */
    @Test
    public void should_execute_ddl_success() throws Exception {
        // given
        String ddl = "CREATE TABLE `ddl_test` (\n" +
                "  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
                "  `UserId` int(11) unsigned NOT NULL COMMENT '用户Id',\n" +
                "PRIMARY KEY (`Id`))" +
                "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='通话记录信息';";

        // when
        JdbcTemplate jdbcTemplate = new JdbcTemplate(databaseInfo);
        jdbcTemplate.executeDDL(ddl);

        // then
        assertThat(jdbcTemplate.query("select * from ddl_test").size(), is(0));
    }
}