package xin.bluesky.leiothrix.server.interactive.client;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * @author 张轲
 */
public class UrlMapping {

    public static void bingServlets(ServletContextHandler context) {

        context.addServlet(new ServletHolder(new SubmitTaskServlet()), "/uploadtaskfile");
        context.addServlet(new ServletHolder(new CancelTaskServlet()), "/cancel");
        context.addServlet(new ServletHolder(new QueryProgressServlet()), "/progress");

    }
}
