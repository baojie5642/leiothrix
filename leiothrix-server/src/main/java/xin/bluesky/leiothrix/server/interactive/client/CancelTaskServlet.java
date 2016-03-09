package xin.bluesky.leiothrix.server.interactive.client;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.msg.ServerMessage;
import xin.bluesky.leiothrix.server.cache.PartitionTaskContainer;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerChannelInboundHandler;
import xin.bluesky.leiothrix.server.storage.TaskStorage;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;

import static java.net.InetAddress.getLocalHost;
import static xin.bluesky.leiothrix.model.msg.ServerMessageType.CANCEL_TASK;

/**
 * @author 张轲
 * @date 16/1/28
 */
public class CancelTaskServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(CancelTaskServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String taskId = request.getParameter("taskId");
        logger.info("接收到client[ip={}]发来的取销任务请求,任务ID为:{}", getLocalHost().getHostAddress(), taskId);

        try {
            TaskStorage.cancel(taskId);
            PartitionTaskContainer.evict(taskId);

            notifyWorkersToExit(taskId);

            // 返回给客户端
            logger.info("任务[taskId={}]取消成功", taskId);
            WebUtils.respond(response, HttpStatus.SC_OK, "OK");
        } catch (Exception e) {
            logger.error("取消任务[taskId={}]时失败,错误信息:{}", taskId, ExceptionUtils.getStackTrace(e));
            WebUtils.respond(response, HttpStatus.SC_INTERNAL_SERVER_ERROR, String.format("取消任务[taskId=%s]失败:%s", taskId, e.getMessage()));
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
