package xin.bluesky.leiothrix.common.jdbc;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.DateUtils2;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.model.db.DatabaseInfo;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author 张轲
 * @date 16/1/19
 */
public class JdbcTemplate {

    private static final Logger logger = LoggerFactory.getLogger(JdbcTemplate.class);

    private static Map<String, DataSource> dataSourceMap = new ConcurrentHashMap();

    private DataSource dataSource;

    public JdbcTemplate(DatabaseInfo databaseInfo) {
        this.dataSource = getDataSource(databaseInfo);
    }

    private synchronized static DataSource getDataSource(DatabaseInfo databaseInfo) {
        String key = buildUrl(databaseInfo);
        if (dataSourceMap.containsKey(key)) {
            return dataSourceMap.get(key);
        } else {
            logger.info("初始化数据源:{}", key);
            DataSource newDataSource = createDataSource(databaseInfo);
            dataSourceMap.put(key, newDataSource);
            return newDataSource;
        }
    }

    private static DataSource createDataSource(DatabaseInfo databaseInfo) {
        PoolProperties poolProperties = new PoolProperties();
        poolProperties.setUrl(buildUrl(databaseInfo));
        poolProperties.setDriverClassName("com.mysql.jdbc.Driver");
        poolProperties.setUsername(databaseInfo.getUserName());
        poolProperties.setPassword(databaseInfo.getPassword());
        poolProperties.setTestWhileIdle(true);
        poolProperties.setMaxActive(100);
        poolProperties.setInitialSize(10);
        poolProperties.setMaxIdle(5);
        poolProperties.setMinIdle(5);
        poolProperties.setValidationQuery("SELECT 1");
        poolProperties.setValidationInterval(30000);

        DataSource dataSource = new DataSource();
        dataSource.setPoolProperties(poolProperties);

        return dataSource;
    }

    private static String buildUrl(DatabaseInfo databaseInfo) {
        return StringUtils2.append(DatabaseInfo.getDialectProtocol(databaseInfo.getDialect()),
                databaseInfo.getIp(), ":",
                databaseInfo.getPort(), "/",
                databaseInfo.getSchema(),
                StringUtils.isNotBlank(databaseInfo.getParams()) ? "?" : "", databaseInfo.getParams());
    }

    public List<JSONObject> query(String sql) {
        logger.debug("执行查询SQL:{}", sql);
        Connection connection = null;
        Statement st = null;
        ResultSet rs = null;
        List<JSONObject> resultList = new ArrayList<>();

        try {
            connection = dataSource.getConnection();
            st = connection.createStatement();
            rs = st.executeQuery(sql);

            while (rs.next()) {
                resultList.add(createData(rs));
            }

        } catch (Exception e) {
            logger.error("SQL:{}", sql);
            throw new JdbcException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ignore) {
            }
        }

        return resultList;
    }

    public List<JSONObject> query(String sql, Object... params) {
        logger.debug("执行SQL:{},params:{}", sql, CollectionsUtils2.toString(params));
        Connection connection = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        List<JSONObject> resultList = new ArrayList<>();

        try {
            connection = dataSource.getConnection();
            st = connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                st.setObject(i + 1, params[i]);
            }
            rs = st.executeQuery();

            while (rs.next()) {
                resultList.add(createData(rs));
            }

        } catch (Exception e) {
            logger.error("SQL:{},params:", sql, params);
            throw new JdbcException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ignore) {
            }
        }

        return resultList;
    }

    /**
     * 插入操作.如果插入成功,且有主键生成,则返回值为新插入记录的主键值,否则返回null.
     *
     * @param sql
     * @param params
     * @return
     */
    public Integer insert(String sql, Object... params) {
        return executeUpdate(sql, params);
    }

    /**
     * 批量插入操作.如果插入成功,且有主键生成,则返回值为新插入记录的主键值的列表,否则返回null.
     *
     * @param sql
     * @param params
     * @return
     */
    public List<Integer> insertBatch(String sql, List<Object[]> params) {
        return executeBatchUpdate(sql, params);
    }

    public void update(String sql, Object... params) {
        executeUpdate(sql, params);
    }

    public void updateBatch(String sql, List<Object[]> params) {
        executeBatchUpdate(sql, params);
    }

    public void delete(String sql, Object... params) {
        executeUpdate(sql, params);
    }

    public void deleteBatch(String sql, List<Object[]> params) {
        executeBatchUpdate(sql, params);
    }

    private Integer executeUpdate(String sql, Object... params) {
        logger.debug("执行SQL:{},params:{}", sql, CollectionsUtils2.toString(params));
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            connection = dataSource.getConnection();
            ps = connection.prepareCall(sql);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ps.executeUpdate();

            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("SQL:{},params:", sql, CollectionsUtils2.toString(params));
            throw new JdbcException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ignore) {
            }
        }

    }

    private List<Integer> executeBatchUpdate(String sql, List<Object[]> params) {
        logger.debug("执行插入SQL:{}", sql);
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            connection = dataSource.getConnection();
            ps = connection.prepareCall(sql);
            for (Iterator<Object[]> iterator = params.iterator(); iterator.hasNext(); ) {
                Object[] pa = iterator.next();
                for (int j = 0; j < pa.length; j++) {
                    ps.setObject(j + 1, pa[j]);
                }
                ps.addBatch();
            }
            ps.executeBatch();

            rs = ps.getGeneratedKeys();
            List<Integer> ids = new ArrayList();
            while (rs.next()) {
                ids.add(rs.getInt(1));
            }
            return ids;
        } catch (Exception e) {
            logger.error("SQL:{},params:{}", sql, CollectionsUtils2.toString(params));
            throw new JdbcException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ignore) {
            }
        }

    }

    private JSONObject createData(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        JSONObject data = new JSONObject();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            switch (metaData.getColumnType(i)) {
                case Types.DATE:
                    data.put(columnName, DateUtils2.formatMedium(rs.getDate(i)));
                    break;
                case Types.TIME:
                    data.put(columnName, rs.getTime(i));
                    break;
                case Types.TIMESTAMP:
                    data.put(columnName, DateUtils2.formatFull(rs.getTimestamp(i)));
                    break;
                case Types.BIGINT:
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.INTEGER:
                case Types.NUMERIC:
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.REAL://MySQL中的Float类型会被JDBC识别为REAL类型
                    data.put(columnName, rs.getString(i));
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    data.put(columnName, rs.getObject(i));
                    break;
                case Types.BIT:
                    final boolean v = rs.getBoolean(i);
                    data.put(columnName, rs.wasNull() ? null : v);
                    break;
                default:
                    throw new JdbcException(String.format("不支持java.sql.Types中类型值为[%s]的字段类型", metaData.getColumnType(i)));
            }
        }
        return data;
    }

    public static void destroy() {
        dataSourceMap.values().forEach(ds -> {
            ds.close();
        });
    }
}
