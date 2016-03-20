package xin.bluesky.leiothrix.server.interactive.client;

import com.alibaba.fastjson.JSON;
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

/**
 * @author 张轲
 */
public class QueryProgressServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(QueryProgressServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String taskId = request.getParameter("taskId");
        logger.info("接收到client[ip={}]发来的查询任务进度请求,任务ID为:{}", getLocalHost().getHostAddress(), taskId);

        TaskProgress progress;
        final TaskStatus status = TaskStorage.getStatus(taskId);
        switch (status) {
            case ALLOCATING:
            case PROCESSING:
                progress = new TaskProgress(status.name(), String.format("任务[taskId=%s]正在执行中", taskId));
                break;
            case FINISHED:
                progress = new TaskProgress(status.name(), String.format("任务[taskId=%s]已执行完毕", taskId));
                break;
            case UNALLOCATED:
                progress = new TaskProgress(status.name(), String.format("任务[taskId=%s]尚未被执行", taskId));
                break;
            case CANCELED:
                progress = new TaskProgress(status.name(), String.format("任务[taskId=%s]已被取消", taskId));
                break;
            case ERROR:
                progress = new TaskProgress(status.name(), String.format("任务[taskId=%s]执行过程中出现了严重错误,已被终止", taskId));
                break;
            default:
                progress = new TaskProgress("", String.format("任务[taskId=%s]不存在,或已经被删除", taskId));
                break;
        }

        progress.setDesc(new StringBuffer(progress.getDesc()).append("\r\n").append(collectInformation(taskId)).toString());
        WebUtils.respond(response, HttpStatus.SC_OK, JSON.toJSONString(progress));
        logger.info("任务[taskId={}]的当前情况是:{}", taskId, progress.getDesc());
    }

    private StringBuffer collectInformation(String taskId) {
        StringBuffer buffer = new StringBuffer();

        TaskStatisticsCollector collector = new TaskStatisticsCollector(taskId);
        buffer.append(collector.collectWorkersInfo());

        buffer.append(collector.collectExecutionStatistics());

        return buffer;
    }
}
