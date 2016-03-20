package xin.bluesky.leiothrix.server.interactive.client;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.net.NetUtils;
import xin.bluesky.leiothrix.common.net.exception.CommandException;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.model.msg.ServerMessage;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerChannelInboundHandler;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.net.InetAddress.getLocalHost;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_OK;
import static xin.bluesky.leiothrix.model.msg.ServerMessageType.CANCEL_TASK;
import static xin.bluesky.leiothrix.model.task.TaskStatus.CANCELED;

/**
 * @author 张轲
 */
public class KillForceTaskProcessorServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(KillForceTaskProcessorServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String taskId = request.getParameter("taskId");
        logger.info("接收到client[ip={}]发来的强制KILL任务的worker进程的请求,任务ID为:{}", getLocalHost().getHostAddress(), taskId);

        try {
            TaskStatus status = TaskStorage.getStatus(taskId);
            if (status != CANCELED) {
                WebUtils.respond(response, SC_OK, "不允许KILL!在执行此操作前必须先cancel任务");
                return;
            }

            List<String> workers = TaskStorage.getAllTaskWorkers(taskId);
            if (CollectionsUtils2.isEmpty(workers)) {
                WebUtils.respond(response, SC_OK, "在强制KILL之前,该任务的所有worker进程已全部正常结束");
                return;
            }

            List<String> failed = new ArrayList();
            for (String ip : workers) {
                List<String> pids = TaskStorage.getWorkerProcessor(taskId, ip);
                if (CollectionsUtils2.isEmpty(pids)) {
                    continue;
                }

                pids.forEach(pid -> {
                    try {
                        NetUtils.killForce(ip, ServerConfigure.get("worker.user"), pid);
                    } catch (CommandException e) {
                        logger.error(e.getMessage(), e);
                        failed.add(ip + "-" + pid);
                    }
                });

            }

            StringBuffer responseBody = new StringBuffer("操作成功,");
            if (failed.isEmpty()) {
                responseBody.append("所有进程都已被强制结束");
            } else {
                responseBody.append(String.format("但部分进程强制结束失败[%s]", CollectionsUtils2.toString(failed)));
            }
            // 返回给客户端
            logger.info("任务[taskId={}]的进程强制结束操作执行完成,结果是:{}", taskId, responseBody.toString());
            WebUtils.respond(response, SC_OK, responseBody.toString());
        } catch (Exception e) {
            logger.error("强制结束进程[taskId={}]的操作失败,错误信息:{}", taskId, ExceptionUtils.getStackTrace(e));
            WebUtils.respond(response, SC_INTERNAL_SERVER_ERROR, String.format("操作失败:%s", e.getMessage()));
        }
    }

    private void notifyWorkersToExit(String taskId) {
        Set<Channel> channelSet = WorkerChannelInboundHandler.getClientsSet();
        channelSet.forEach(channel -> {
            ServerMessage message = new ServerMessage(CANCEL_TASK, taskId);
            channel.writeAndFlush(JSON.toJSONString(message) + "\r\n");
        });
    }
}
