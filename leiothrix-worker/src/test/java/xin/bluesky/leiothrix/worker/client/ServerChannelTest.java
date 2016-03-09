package xin.bluesky.leiothrix.worker.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.Ignore;
import org.junit.Test;
import xin.bluesky.leiothrix.common.net.NioServer;

/**
 * @author 张轲
 */
// 需要观察日志来判定,且该类基本不会变动,所以ignore.如需测试,手动打开并观察日志来验证
@Ignore
public class ServerChannelTest {

    @Test
    public void should_send_correct() throws Exception {
        // given
        int serverPort = 8000;

        // when
        new Thread(() -> {
            TestServer.start(serverPort);
        }).start();
        Thread.sleep(1000);

        ServerChannel.connect(new String[]{"localhost", "127.0.0.1"}, serverPort);
        ServerChannel.send("I am a good boy");

        Thread.sleep(1 * 1000);
        System.out.println("上面只应该输出{I am a good boy}");
    }

    @Test
    public void should_send_correct_if_first_server_fail() throws Exception {
        // given
        int serverPort = 8000;

        // when
        new Thread(() -> {
            TestServer.start(serverPort);
        }).start();
        Thread.sleep(1000);

        ServerChannel.connect(new String[]{"unknowhost", "localhost"}, serverPort);
        ServerChannel.send("I am a bad boy");
        Thread.sleep(1 * 1000);
        System.out.println("上面应该输出{发送消息给主server失败,改发送备用server}和{I am a bad boy}");

        // 再次发送消息,则不应该再有错误日志,因为已经把失败的server移除了
        ServerChannel.send("second time");
        Thread.sleep(1 * 1000);
        System.out.println("上面只应该输出{second time}");
    }

    private static class TestServer {
        public static void start(int port) {
            NioServer server = new NioServer(port, ServerHandler.class);
            System.out.println("server 1 startup.Listening on 8000");
            server.start();
        }

        public static class ServerHandler extends ChannelInboundHandlerAdapter {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                System.out.println("server1:" + msg);
            }
        }
    }
}