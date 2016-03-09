package xin.bluesky.leiothrix.server.interactive.client;

import com.alibaba.fastjson.JSON;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author 张轲
 */
public class WebUtils {

    public static void respond(HttpServletResponse resp, int statusCode, String content) throws IOException {
        resp.setCharacterEncoding("UTF-8");
        resp.setStatus(statusCode);
        resp.getWriter().print(content);
        resp.getWriter().flush();
    }

    public static void respond(HttpServletResponse resp, int statusCode, Object content) throws IOException {
        respond(resp, statusCode, JSON.toJSONString(content));
    }
}
