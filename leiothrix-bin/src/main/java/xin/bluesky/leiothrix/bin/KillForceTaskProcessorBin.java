package xin.bluesky.leiothrix.bin;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张轲
 */
public class KillForceTaskProcessorBin {

    public static final Logger logger = LoggerFactory.getLogger(KillForceTaskProcessorBin.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("参数不足.正确参数应该是:serverIp serverPort taskId ");
        }

        String serverIp = args[0];
        int serverPort = Integer.parseInt(args[1]);
        String taskId = args[2];

        System.out.println("如执行cancel任务的请求未能关闭worker进程,再执行此操作");
        String response = killForce(serverIp, serverPort, taskId);
        System.out.println(response);
    }

    private static String killForce(String serverIp, int serverPort, String taskId) throws Exception {
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        try {
            client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost("http://" + serverIp + ":" + serverPort + "/progress");
            List<NameValuePair> params = new ArrayList();
            params.add(new BasicNameValuePair("taskId", taskId));
            post.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
            if (statusCode == HttpStatus.SC_OK) {
                return responseBody;
            } else {
                throw new Exception(responseBody);
            }
        } finally {
            if (client != null) {
                client.close();
            }
            if (response != null) {
                response.close();
            }
        }
    }
}
