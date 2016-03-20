package xin.bluesky.leiothrix.model.task.partition;

/**
 * @author 张轲
 */
public class ExecutionStatistics {

    private int handledRecordNum;

    private int successRecordNum;

    private int failRecordNum;

    private String failPageName;

    private String exceptionMsg;

    private long queryUsingTime;

    private long handleUsingTime;

    private long totalTime;

    public ExecutionStatistics() {
    }

    public ExecutionStatistics(int handledRecordNum, int successRecordNum, int failRecordNum, String failPageName, String exceptionMsg, long queryUsingTime, long handleUsingTime, long totalTime) {
        this.handledRecordNum = handledRecordNum;
        this.successRecordNum = successRecordNum;
        this.failRecordNum = failRecordNum;
        this.failPageName = failPageName;
        this.exceptionMsg = exceptionMsg;
        this.queryUsingTime = queryUsingTime;
        this.handleUsingTime = handleUsingTime;
        this.totalTime = totalTime;
    }

    public int getHandledRecordNum() {
        return handledRecordNum;
    }

    public void setHandledRecordNum(int handledRecordNum) {
        this.handledRecordNum = handledRecordNum;
    }

    public String getExceptionMsg() {
        return exceptionMsg;
    }

    public void setExceptionMsg(String exceptionMsg) {
        this.exceptionMsg = exceptionMsg;
    }

    public long getQueryUsingTime() {
        return queryUsingTime;
    }

    public void setQueryUsingTime(long queryUsingTime) {
        this.queryUsingTime = queryUsingTime;
    }

    public long getHandleUsingTime() {
        return handleUsingTime;
    }

    public void setHandleUsingTime(long handleUsingTime) {
        this.handleUsingTime = handleUsingTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    public int getSuccessRecordNum() {
        return successRecordNum;
    }

    public void setSuccessRecordNum(int successRecordNum) {
        this.successRecordNum = successRecordNum;
    }

    public int getFailRecordNum() {
        return failRecordNum;
    }

    public void setFailRecordNum(int failRecordNum) {
        this.failRecordNum = failRecordNum;
    }

    public String getFailPageName() {
        return failPageName;
    }

    public void setFailPageName(String failPageName) {
        this.failPageName = failPageName;
    }
}
