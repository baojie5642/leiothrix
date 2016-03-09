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

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张轲
 */
public class CancelTaskBin {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("参数不足.正确参数应该是:serverIp serverPort taskId ");
        }

        String serverIp = args[0];
        int serverPort = Integer.parseInt(args[1]);
        String taskId = args[2];

        cancel(serverIp, serverPort, taskId);
    }

    private static void cancel(String serverIp, int serverPort, String taskId) throws Exception {
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        try {
            client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost("http://" + serverIp + ":" + serverPort + "/cancel");
            List<NameValuePair> params = new ArrayList();
            params.add(new BasicNameValuePair("taskId", taskId));
            post.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
            if (statusCode == HttpStatus.SC_OK) {
                System.out.println(String.format("task[taskId=%s]取消成功", taskId));
            } else {
                throw new Exception(String.format("task[taskId=%s]取消失败,返回信息为:%s", taskId, responseBody));
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
