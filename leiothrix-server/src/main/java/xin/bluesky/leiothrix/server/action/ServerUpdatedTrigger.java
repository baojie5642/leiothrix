package xin.bluesky.leiothrix.server.action;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import xin.bluesky.leiothrix.model.msg.ServerMessage;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.server.interactive.worker.WorkerChannelInboundHandler;

import java.util.List;
import java.util.Set;

import static xin.bluesky.leiothrix.model.msg.ServerMessageType.SERVER_UPDATED;

/**
 * server列表发生变化时,通知所有worker更新.
 *
 * @author 张轲
 */
public class ServerUpdatedTrigger {

    public void trigger(List<String> allServers) {
        Set<Channel> clientsSet = WorkerChannelInboundHandler.getClientsSet();
        if (CollectionsUtils2.isEmpty(clientsSet)) {
            return;
        }

        clientsSet.forEach((c) -> {
            ServerMessage message = new ServerMessage(SERVER_UPDATED, toString(allServers));
            c.writeAndFlush(JSON.toJSONString(message) + "\r\n");
        });
    }

    private String toString(List<String> list) {
        if (CollectionsUtils2.isEmpty(list)) {
            return "";
        }

        StringBuffer buffer = new StringBuffer("");
        list.forEach(item -> {
            buffer.append(item.toString()).append(",");
        });
        buffer.deleteCharAt(buffer.length() - 1).append("");

        return buffer.toString();

    }
}
