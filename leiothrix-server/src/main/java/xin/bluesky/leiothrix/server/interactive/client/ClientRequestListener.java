package xin.bluesky.leiothrix.server.interactive.client;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

/**
 * @author 张轲
 * @date 16/1/29
 */
public class ClientRequestListener {

    public static void start(int port) throws Exception {
        Server server = new Server(port);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);

        UrlMapping.bingServlets(context);
        server.start();
    }
}
