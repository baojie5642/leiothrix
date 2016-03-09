package xin.bluesky.leiothrix.server.interactive.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.interactive.worker.CommandFactory;
import xin.bluesky.leiothrix.server.storage.ServerStorage;
import xin.bluesky.leiothrix.server.util.LeiothrixUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author 张轲
 * @date 16/3/7
 */
public class TaskFileService {

    private static final Logger logger = LoggerFactory.getLogger(TaskFileService.class);

    private static final ExecutorService executorService = new ThreadPoolExecutor(3, 10, 1, MINUTES, new ArrayBlockingQueue(10),
            new ThreadFactoryBuilder().setNameFormat("task-file-service-pool-%d").build(), new ThreadPoolExecutor.CallerRunsPolicy());

    public static boolean scp2OtherServers(String taskId, File taskFileDir) {
        List<TransferResult> results = new ArrayList();

        List<String> servers = ServerStorage.getAllServers();
        CountDownLatch latch = new CountDownLatch(servers.size() - 1);

        for (String server : servers) {
            if (server.equals(LeiothrixUtils.getMyIp())) {
                continue;
            }
            executorService.submit(new TransferTask(taskId, taskFileDir, server, taskFileDir.getParent(), results, latch));
        }

        try {
            latch.await();
            for (TransferResult result : results) {
                if (!result.isSuccess()) {
                    logger.error("传输jar到其他server时出错,错误信息为:{}", CollectionsUtils2.toString(results));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            return false;
        }

        return true;

    }

    public static void deleteOnAllServers(String taskId) {
        List<String> servers = ServerStorage.getAllServers();

        for (String server : servers) {
            executorService.submit(new DeleteTask(taskId, server, LeiothrixUtils.getTaskDirPath(taskId).getAbsolutePath()));
        }
    }

    private static class TransferTask implements Runnable {

        private String taskId;

        private File taskFileDir;

        private String remoteServerIp;

        private String remotePath;

        private List<TransferResult> results;

        private CountDownLatch latch;

        public TransferTask(String taskId, File taskFileDir, String remoteServerIp,
                            String remotePath, List<TransferResult> results, CountDownLatch latch) {
            this.taskId = taskId;
            this.taskFileDir = taskFileDir;
            this.remoteServerIp = remoteServerIp;
            this.remotePath = remotePath;
            this.results = results;
            this.latch = latch;
        }

        @Override
        public void run() {
            String command = StringUtils2.append("scp -r ", taskFileDir.getAbsolutePath(), " ",
                    ServerConfigure.get("server.user"), "@", remoteServerIp, ":", remotePath);

            try {
                int exitValue = Runtime.getRuntime().exec(command).waitFor();
                if (exitValue != 0) {
                    logger.error("为任务[taskId={}]拷贝jar到server[ip={}]时失败,命令为:{},返回的错误码为", taskId, remoteServerIp, command, exitValue);
                    results.add(new TransferResult(false, "错误码:" + exitValue));
                    latch.countDown();
                    return;
                }
            } catch (Exception e) {
                logger.error("为任务[taskId={}]拷贝jar到server[ip={}]时失败,命令为:{},错误信息为:{}", taskId, remoteServerIp, command, ExceptionUtils.getStackTrace(e));
                results.add(new TransferResult(false, "错误信息:" + e.getMessage()));
                latch.countDown();
                return;
            }

            results.add(new TransferResult(true));
            latch.countDown();
        }
    }

    private static class TransferResult {

        private boolean success;

        private String errorMsg;

        public TransferResult(boolean success) {
            this.success = success;
        }

        public TransferResult(boolean success, String errorMsg) {
            this.success = success;
            this.errorMsg = errorMsg;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    private static class DeleteTask implements Runnable {

        private String taskId;

        private String remoteServerIp;

        private String remotePath;

        public DeleteTask(String taskId, String remoteServerIp, String remotePath) {
            this.taskId = taskId;
            this.remotePath = remotePath;
            this.remoteServerIp = remoteServerIp;
        }

        @Override
        public void run() {
            String command = StringUtils2.append("rm -rf ", remotePath);
            String remoteFullCommand = CommandFactory.getRemoteFullCommand(command, ServerConfigure.get("server.user"), remoteServerIp);

            try {
                Process process;
                if (remoteServerIp.equals(LeiothrixUtils.getMyIp())) {
                    process = Runtime.getRuntime().exec(command);
                } else {
                    process = Runtime.getRuntime().exec(remoteFullCommand);
                }
                int exitValue = process.waitFor();
                if (exitValue != 0) {
                    logger.error("在server[ip={}]上删除任务目录[taskId={}]时失败,命令为:{},返回的错误码为", taskId, remoteServerIp, command, exitValue);
                    return;
                }
            } catch (Exception e) {
                logger.error("在server[ip={}]上删除任务目录[taskId={}]时失败,命令为:{},返回的错误码为", taskId, remoteServerIp, command, ExceptionUtils.getStackTrace(e));
                return;
            }

        }
    }
}
