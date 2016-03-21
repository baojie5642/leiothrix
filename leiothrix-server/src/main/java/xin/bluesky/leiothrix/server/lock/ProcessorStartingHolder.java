package xin.bluesky.leiothrix.server.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 张轲
 */
public class ProcessorStartingHolder {

    private static final Map<String, ProcessorStartingHolder> holders = new ConcurrentHashMap<>();

    private String taskId;

    private ReentrantLock lock;

    private Condition condition;

    private boolean runningSuccess;

    private String errorMsg;

    private ProcessorStartingHolder(String taskId) {
        this.taskId = taskId;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    public synchronized static ProcessorStartingHolder get(String taskId) {
        ProcessorStartingHolder holder = holders.get(taskId);
        if (holder == null) {
            holder = new ProcessorStartingHolder(taskId);
            holders.put(taskId, holder);
        }
        return holder;
    }

    public void hold() throws InterruptedException {
        lock.lock();
        condition.await();
    }

    public boolean hold(long time, TimeUnit timeUnit) {
        try {
            lock.lock();
            return condition.await(time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void unHold() {
        lock.lock();
        condition.signal();
        lock.unlock();
    }

    public boolean isRunningSuccess() {
        return runningSuccess;
    }

    public void setRunningSuccess(boolean runningSuccess) {
        this.runningSuccess = runningSuccess;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
