package xin.bluesky.leiothrix.common.net;

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.net.exception.RemoteException;
import xin.bluesky.leiothrix.common.util.CollectionsUtils2;

/**
 * Created by zhangke on 15/9/1.
 */
public class NioClient {
    private static final Logger logger = LoggerFactory.getLogger(NioClient.class);

    private Class<? extends ChannelInboundHandler> channelInboundHandlerClass;

    private ChannelFuture channelFuture;

    private EventLoopGroup workerGroup = new NioEventLoopGroup();

    public NioClient(Class<? extends ChannelInboundHandler> channelInboundHandlerClass) {
        this.channelInboundHandlerClass = channelInboundHandlerClass;
    }

    public void connect(String[] ips, int port) {
        boolean connected = false;
        for (int i = 0; i < ips.length; i++) {
            try {
                connect(ips[i], port);

                connected = true;
                logger.info("成功连接到{}", ips[i]);

                break;
            } catch (Exception e1) {
                logger.warn("连接{}失败", ips[i]);
            }
        }

        if (!connected) {
            throw new RemoteException(String.format("未能成功连接上[%s]中任何一个", CollectionsUtils2.toString(ips)));
        }
    }

    public void connect(String ip, int port) throws Exception {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new LineBasedFrameDecoder(2048))
                                .addLast("encoder", new StringEncoder())
                                .addLast("decoder", new StringDecoder())
                                .addLast("handler", channelInboundHandlerClass.newInstance());
                    }
                });

        this.channelFuture = bootstrap.connect(ip, port).sync();
    }

    public ChannelFuture send(Object message) {
        return channelFuture.channel().writeAndFlush(JSON.toJSONString(message) + "\r\n");
    }

    public boolean isActive() {
        return channelFuture.channel().isActive();
    }

    public void shutdown() {
        try {
            if (channelFuture != null) {
                channelFuture.channel().close().sync();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
        } catch (InterruptedException e) {
            logger.error("线程被中断", ExceptionUtils.getStackTrace(e));
        }
    }
}
