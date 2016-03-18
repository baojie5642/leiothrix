package xin.bluesky.leiothrix.server.interactive.client;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.task.TaskProgress;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.action.TaskStatisticsCollector;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static java.net.InetAddress.getLocalHost;
import static xin.bluesky.leiothrix.model.task.TaskStatus.PROCESSING;

/**
 * @author 张轲
 */
public class QueryProgressServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(QueryProgressServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String taskId = request.getParameter("taskId");
        logger.info("接收到client[ip={}]发来的查询任务进度请求,任务ID为:{}", getLocalHost().getHostAddress(), taskId);

        TaskProgress progress;
        try {
            final TaskStatus status = TaskStorage.getStatus(taskId);
            switch (status) {
                case ALLOCATING:
                case PROCESSING:
                    progress = new TaskProgress(PROCESSING.name(), String.format("任务[taskId=%s]正在执行中", taskId));
                    break;
                case FINISHED:
                    progress = new TaskProgress(PROCESSING.name(), String.format("任务[taskId=%s]已执行完毕", taskId));
                    break;
                case UNALLOCATED:
                    progress = new TaskProgress(PROCESSING.name(), String.format("任务[taskId=%s]尚未被执行", taskId));
                    break;
                case CANCELED:
                    progress = new TaskProgress(PROCESSING.name(), String.format("任务[taskId=%s]已被取消", taskId));
                    break;
                case ERROR:
                    progress = new TaskProgress(PROCESSING.name(), String.format("任务[taskId=%s]执行过程中出现了严重错误,已被终止", taskId));
                    break;
                default:
                    progress = new TaskProgress(PROCESSING.name(), String.format("任务[taskId=%s]不存在,或已经被删除", taskId));
                    break;
            }

            progress.setDesc(new StringBuffer(progress.getDesc()).append("\r\n").append(collectInformation(taskId)).toString());
            WebUtils.respond(response, HttpStatus.SC_OK, progress.getDesc());
            logger.info(progress.getDesc());
        } catch (Exception e) {
            logger.error("查询任务进度[taskId={}]失败,错误信息:{}", taskId, ExceptionUtils.getStackTrace(e));
            WebUtils.respond(response, HttpStatus.SC_INTERNAL_SERVER_ERROR, String.format("查询任务进度[taskId=%s]失败:%s", taskId, e.getMessage()));
        }

    }

    private StringBuffer collectInformation(String taskId) {
        StringBuffer buffer = new StringBuffer();

        TaskStatisticsCollector collector=new TaskStatisticsCollector(taskId);
        buffer.append(collector.collectWorkersInfo());

        buffer.append(collector.collectExecutionStatistics());

        return buffer;
    }
}
