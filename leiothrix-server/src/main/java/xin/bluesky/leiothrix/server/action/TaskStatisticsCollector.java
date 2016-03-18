package xin.bluesky.leiothrix.server.action;

import org.apache.commons.lang3.StringUtils;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.task.partition.ExecutionStatistics;
import xin.bluesky.leiothrix.server.storage.RangeStorage;
import xin.bluesky.leiothrix.server.storage.TableStorage;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static xin.bluesky.leiothrix.common.util.DateUtils2.string2DateFull;

/**
 * @author 张轲
 */
public class TaskStatisticsCollector {

    private String taskId;

    public TaskStatisticsCollector(String taskId) {
        this.taskId = taskId;
    }

    public StringBuffer collectWorkersInfo() {
        StringBuffer buffer = new StringBuffer();

        List<String> workers = TaskStorage.getAllTaskWorkers(taskId);
        if (CollectionsUtils2.isEmpty(workers)) {
            return buffer;
        }

        final List<WorkerInfo> workerInfoList = new ArrayList();
        workers.forEach(ip -> {
            WorkerInfo workerInfo = new WorkerInfo(ip);
            List<String> processorIds = TaskStorage.getWorkerProcessor(taskId, ip);

            workerInfo.setProcessors(from(processorIds).transform(processorId -> {
                String startTime = TaskStorage.getWorkerProcessorStartTime(taskId, ip, processorId);
                String finishedTime = TaskStorage.getWorkerProcessorFinishedTime(taskId, ip, processorId);
                return new ProcessorInfo(processorId, startTime, finishedTime);
            }).toList());

            workerInfoList.add(workerInfo);
        });

        int allProcessorNum = 0;
        for (WorkerInfo workerInfo : workerInfoList) {
            allProcessorNum += workerInfo.getProcessors().size();
        }

        buffer.append(String.format("共有%s台worker物理机%s个进程在运行该任务[taskId=%s],详细情况如下:\r\n", workers.size(), allProcessorNum, taskId));
        workerInfoList.forEach(workerInfo -> {
            buffer.append(String.format("worker:%s,有%s个进程,分别是:\r\n", workerInfo.getIp(), workerInfo.getProcessors().size()));
            workerInfo.getProcessors().forEach(p -> {
                buffer.append(String.format("   PID=%s(启动时间[%s],", p.getPid(), p.getStartTime()))
                        .append(p.isFinished() ? String.format("结束时间[%s],", p.getFinishedTime()) : "还未结束,")
                        .append(String.format("耗时[%s]毫秒)", p.getRunningTime() / 1000))
                        .append("\r\n");
            });
        });

        return buffer;
    }

    public StringBuffer collectExecutionStatistics() {
        StringBuffer result = new StringBuffer();

        int taskHandledRecordNum = 0, taskSuccessRecordNum = 0, taskFailRecordNum = 0;
        long taskTotalTime = 0, taskQueryTime = 0, taskHandleTime = 0;

        List<String> tableNameList = TableStorage.getAllTablesByTaskId(taskId);
        StringBuffer tmp = new StringBuffer();
        for (String tableName : tableNameList) {
            int tableHandledRecordNum = 0, tableSuccessRecordNum = 0, tableFailRecordNum = 0;
            long tableTotalTime = 0, tableQueryTime = 0, tableHandleTime = 0;
            StringBuffer tableFailPage = new StringBuffer(), tableExceptionStackTrace = new StringBuffer();

            List<String> rangeNameList = RangeStorage.getAllRangesByTableName(taskId, tableName);
            tmp.append(String.format("表[%s]共有%s个任务片,", tableName, rangeNameList.size()));

            for (String rangeName : rangeNameList) {
                ExecutionStatistics stat = RangeStorage.getExecutionStatistics(taskId, tableName, rangeName);

                tableHandledRecordNum += stat.getHandledRecordNum();
                tableSuccessRecordNum += stat.getSuccessRecordNum();
                tableFailRecordNum += stat.getFailRecordNum();

                tableTotalTime += stat.getTotalTime();
                tableQueryTime += stat.getQueryUsingTime();
                tableHandleTime += stat.getHandleUsingTime();

                if (StringUtils.isNotBlank(stat.getFailPageName())) {
                    tableFailPage.append(stat.getFailPageName());
                }
                if (StringUtils.isNotBlank(stat.getExceptionStackTrace())) {
                    tableExceptionStackTrace.append(stat.getExceptionStackTrace());
                }
            }
            tmp.append(String.format("处理数据%s条,成功%s条,失败%s条;处理时间%s秒,查询耗时%s秒,应用端处理耗时%s秒,平均每秒处理%s条数据.",
                    tableHandledRecordNum, tableSuccessRecordNum, tableFailRecordNum,
                    tableTotalTime / 1000, tableQueryTime / 1000, tableHandleTime / 1000,
                    tableHandledRecordNum / (tableTotalTime / 1000)));
            if (tableFailPage.length() > 0) {
                tmp.append(String.format("失败任务片[%s],异常信息:\r\n%s", tableFailPage, tableExceptionStackTrace));
            }
            tmp.append("\r\n");

            taskHandledRecordNum += tableHandledRecordNum;
            taskSuccessRecordNum += tableSuccessRecordNum;
            taskFailRecordNum += tableFailRecordNum;

            taskTotalTime += tableTotalTime;
            taskQueryTime += tableQueryTime;
            taskHandleTime += tableHandleTime;
        }

        result.append(String.format("任务[taskId=%s]累计处理数据%s条,成功%s条,失败%s条;累计处理时间%s秒,查询耗时%s秒,应用端处理耗时%s秒,平均每秒处理%s条数据\"",
                taskId, taskHandledRecordNum, taskSuccessRecordNum, taskFailRecordNum,
                taskTotalTime / 1000, taskQueryTime / 1000, taskHandleTime / 1000,
                taskHandledRecordNum / (taskTotalTime / 1000)))
                .append("\r\n");
        result.append(tmp);

        return result;
    }

    public class WorkerInfo {

        private String ip;

        private List<ProcessorInfo> processors;

        public WorkerInfo(String ip) {
            this.ip = ip;
        }

        public String getIp() {
            return ip;
        }

        public List<ProcessorInfo> getProcessors() {
            return processors;
        }

        public void setProcessors(List<ProcessorInfo> processors) {
            this.processors = processors;
        }
    }

    public class ProcessorInfo {

        private String pid;

        private String startTime;

        private String finishedTime;

        public ProcessorInfo(String pid, String startTime, String finishedTime) {
            this.pid = pid;
            this.startTime = startTime;
            this.finishedTime = finishedTime;
        }

        public String getPid() {
            return pid;
        }

        public String getStartTime() {
            return startTime;
        }

        public String getFinishedTime() {
            return finishedTime;
        }

        public boolean isFinished() {
            return finishedTime != null;
        }

        public long getRunningTime() {
            if (isFinished()) {
                return string2DateFull(finishedTime).getTime() - string2DateFull(startTime).getTime();
            } else {
                return new Date().getTime() - string2DateFull(startTime).getTime();
            }
        }
    }
}
