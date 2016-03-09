package xin.bluesky.leiothrix.server.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 元素会超时的map,主要是为了定时清理掉可能不再需要的数据.
 *
 * 各元素的超时时间是统一在创建{@link ExpiredMap}时设置的,单位为秒.
 * 如果当前时间离对象放入时间超过了阀值,则会被从map中移除掉.
 *
 * @author 张轲
 */
//todo: 最好要限制这个cache的大小,至少是元素数量
public class ExpiredMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ExpiredMap.class);

    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("expred-map-clean-%d").build());

    protected static long SCAN_INTERVAL = 10;

    private static final List<ExpiredMap> allExpiredMap = new ArrayList();

    private int timeout;

    private ConcurrentHashMap<K, Entity<V>> map = new ConcurrentHashMap<>();

    static {
        executor.scheduleAtFixedRate(new Cleaner(), 0, SCAN_INTERVAL, TimeUnit.SECONDS);
    }

    public ExpiredMap(int timeout) {
        this.timeout = timeout;
        allExpiredMap.add(this);
    }

    public void put(K key, V value) {
        Entity<V> entity = new Entity<>(value);
        map.put(key, entity);
    }

    public V putIfAbsent(K key, V value) {
        Entity<V> entity = new Entity<>(value);
        Entity<V> old = map.putIfAbsent(key, entity);
        if (old == null) {
            return null;
        }
        return old.get();
    }

    public V get(K key) {
        Entity<V> entity = map.get(key);
        if (entity == null) {
            return null;
        }
        return entity.get();
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public V remove(K key) {
        Entity<V> entity = map.remove(key);
        return entity.get();
    }

    private Set<Map.Entry<K, Entity<V>>> entrySet() {
        return map.entrySet();
    }

    private int getTimeout() {
        return timeout;
    }

    private class Entity<V> {
        private long createTime = new Date().getTime();

        private V value;

        public Entity(V value) {
            this.value = value;
        }

        public V get() {
            return value;
        }

        public long getCreateTime() {
            return createTime;
        }
    }

    private static class Cleaner implements Runnable {
        @Override
        public void run() {
            int totalRemoveCount = 0;
            for (ExpiredMap expiredMap : allExpiredMap) {
                List<Map.Entry> list = new ArrayList(expiredMap.entrySet());
                int removeCount = doClean(expiredMap, list);
                totalRemoveCount += removeCount;
            }

            if (totalRemoveCount != 0) {
                logger.debug("本次总共删除了{}个元素", totalRemoveCount);
            }
        }

        private int doClean(ExpiredMap expiredMap, List<Map.Entry> list) {
            int count = 0;
            long now = new Date().getTime();
            for (Map.Entry entry : list) {
                ExpiredMap.Entity entity = (ExpiredMap.Entity) entry.getValue();
                if (now - entity.getCreateTime() > expiredMap.getTimeout() * 1000) {
                    expiredMap.remove(entry.getKey());
                    count++;
                }
            }
            return count;
        }
    }
}
