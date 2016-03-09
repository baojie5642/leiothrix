package xin.bluesky.leiothrix.worker.client;

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;
import xin.bluesky.leiothrix.worker.msghandler.ServerChannelInboundHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static xin.bluesky.leiothrix.worker.client.ChannelStatus.BROKEN;
import static xin.bluesky.leiothrix.worker.client.ChannelStatus.NORMAL;

/**
 * @author 张轲
 * @date 16/1/25
 */
public class ServerChannel {

    private static final Logger logger = LoggerFactory.getLogger(ServerChannel.class);

    private static EventLoopGroup workerGroup = new NioEventLoopGroup();

    private static List<ChannelPool> poolList = new ArrayList();

    private static final Bootstrap bootstrap = new Bootstrap();

    private static int serverPort;

    private ServerChannel() {

    }

    public static void connect(String[] serversIp, int port) {
        ServerChannel.serverPort = port;
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true);

        Arrays.asList(serversIp).forEach((ip) -> {
            InetSocketAddress addr = new InetSocketAddress(ip, port);
            poolList.add(new ChannelPool(ip, new SimpleChannelPool(bootstrap.remoteAddress(addr), new ServerChannelPoolHandler())));
        });
        logger.info("创建netty连接池,server为:{}", CollectionsUtils2.toString(serversIp));
    }

    private static void addServer(String ip, int port) {
        InetSocketAddress addr = new InetSocketAddress(ip, port);

        for (ChannelPool cp : poolList) {
            if (cp.getIp().equals(ip)) {
                if (cp.getChannelStatus() == NORMAL) {
                    return;
                } else if (cp.getChannelStatus() == BROKEN) {
                    cp = new ChannelPool(ip, new SimpleChannelPool(bootstrap.remoteAddress(addr), new ServerChannelPoolHandler()));
                    logger.info("增加新的server:{}", ip);
                    return;
                }
            }
        }

        // 如果走到这,则表明在pool中没有相同的ip
        poolList.add(new ChannelPool(ip, new SimpleChannelPool(bootstrap.remoteAddress(addr), new ServerChannelPoolHandler())));
        logger.info("增加新的server:{}", ip);
    }

    public static void send(Object message) {
        final String body = JSON.toJSONString(message);
        send(body, 0);
    }

    private static void send(String body, final int channelIndex) {
        if (channelIndex == poolList.size()) {
            return;
        }

        final ChannelPool cp = poolList.get(channelIndex);
        // 如果该channel不通,则继续向后递归
        if (cp.getChannelStatus() == BROKEN) {
            send(body, channelIndex + 1);
        }

        SimpleChannelPool sp = cp.getChannelPool();
        sp.acquire().addListener((new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> future) throws Exception {
                if (future.isSuccess()) {
                    Channel ch = future.getNow();
                    ch.writeAndFlush(body + "\r\n");
                    sp.release(ch);
                } else {
                    // 如此次操作失败,则判定server不可用,置其为不可用状态
                    if (channelIndex == poolList.size() - 1) {
                        logger.error("发送消息[{}]给所有server[{}]都失败,异常为:{}", body, CollectionsUtils2.toString(poolList), ExceptionUtils.getStackTrace(future.cause()));
                    } else {
                        logger.warn("发送消息[{}]给server[{}]失败,异常为:{}", body, cp.getIp(), ExceptionUtils.getStackTrace(future.cause()));
                    }
                    sp.close();
                    // 在多线程+异步+嵌套调用的环境下,删除iterator中的元素相当麻烦,所以设置状态.由于worker进程终究会结束,所以这里也不做清理broken状态的过程
                    cp.setChannelStatus(BROKEN);
                    send(body, channelIndex + 1);
                }
            }
        }));
    }

    /**
     * server发生变化时,与内存中的server列表同步.
     * 主要是增加server时(往往是server down之后又重启了)需要同步.如果server crash,worker在netty连接时会自动发现失效,所以不需处理
     *
     * @param allServers
     */
    public static void serverChanged(List<String> allServers) {
        allServers.forEach((newIp) -> {
                    addServer(newIp, serverPort);
                }
        );
    }

    public static void shutdown() throws InterruptedException {
        if (!CollectionsUtils2.isEmpty(poolList)) {
            poolList.forEach(pool -> {
                pool.getChannelPool().close();
            });
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        logger.info("关闭与server通信的client线程");
    }

    public static class ServerChannelPoolHandler extends AbstractChannelPoolHandler {
        @Override
        public void channelCreated(Channel ch) throws Exception {
            ch.pipeline()
                    .addLast(new LineBasedFrameDecoder(2048))
                    .addLast("encoder", new StringEncoder())
                    .addLast("decoder", new StringDecoder())
                    .addLast("handler", new ServerChannelInboundHandler());
        }
    }
}
