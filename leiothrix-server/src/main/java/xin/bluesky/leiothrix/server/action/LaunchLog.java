package xin.bluesky.leiothrix.server.action;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author 张轲
 */
class LaunchLog {

    private Map<String, LogDetail> map = new HashMap();

    private LogDetail createLogDetailIfAbsent(String workerIp) {
        LogDetail detail = map.get(workerIp);
        if (detail == null) {
            detail = new LogDetail();
            map.put(workerIp, detail);
        }
        return detail;
    }

    public void addJarPath(String workerIp, String jarPath) {
        LogDetail detail = createLogDetailIfAbsent(workerIp);
        detail.setJarPath(jarPath);
    }

    public void incProcessorNum(String workerIp) {
        LogDetail detail = createLogDetailIfAbsent(workerIp);
        detail.setProcessorNum(detail.getProcessorNum() + 1);
    }

    public void setAvailableProcessNumBeforeLuanch(String workerIp, int numBeforeLuanch) {
        LogDetail detail = createLogDetailIfAbsent(workerIp);
        detail.setProcessNumBeforeLaunch(numBeforeLuanch);
    }

    public String getJarPath(String workerIp) {
        LogDetail detail = createLogDetailIfAbsent(workerIp);
        return detail.getJarPath();
    }

    public int getProcessorNum(String workerIp) {
        LogDetail detail = createLogDetailIfAbsent(workerIp);
        return detail.getProcessorNum();
    }

    public int getAvailableProcessNumBeforeLuanch(String workerIp) {
        LogDetail detail = createLogDetailIfAbsent(workerIp);
        return detail.getProcessNumBeforeLaunch();
    }

    public int getTotalProcessorNum() {
        int totalProcessorNum = 0;
        for (Iterator<LogDetail> iterator = map.values().iterator(); iterator.hasNext(); ) {
            totalProcessorNum += iterator.next().getProcessorNum();
        }
        return totalProcessorNum;
    }

    private class LogDetail {

        private String jarPath;

        private int processNumBeforeLaunch;

        private int processorNum;

        public String getJarPath() {
            return jarPath;
        }

        public void setJarPath(String jarPath) {
            this.jarPath = jarPath;
        }

        public int getProcessNumBeforeLaunch() {
            return processNumBeforeLaunch;
        }

        public void setProcessNumBeforeLaunch(int processNumBeforeLaunch) {
            this.processNumBeforeLaunch = processNumBeforeLaunch;
        }

        public int getProcessorNum() {
            return processorNum;
        }

        public void setProcessorNum(int processorNum) {
            this.processorNum = processorNum;
        }
    }
}
