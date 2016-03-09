package xin.bluesky.leiothrix.bin;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.FormBodyPartBuilder;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.model.bin.SubmitResponse;
import xin.bluesky.leiothrix.model.bin.SubmitStatus;

import java.io.File;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

/**
 * @author 张轲
 * @date 16/1/27
 */
public class SubmitTaskBin {

    private static final Logger logger = LoggerFactory.getLogger(SubmitTaskBin.class);

    //todo: 检查config.json的格式是否正确
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            throw new IllegalArgumentException("参数不足.正确参数应该是:serverIp serverPort jar文件绝对路径 config.json的绝对路径 jar文件中执行的main类 ");
        }

        String serverIp = args[0];
        int serverPort = Integer.parseInt(args[1]);
        logger.info("开始提交任务,服务端信息:[ip={},port={}]", serverIp, serverPort);

        // 提交请求,包括上传jar包,配置文件和指定启动类
        String jarFile = args[2];
        String configFile = args[3];
        String mainClass = args[4];

        String respBody = submit(serverIp, serverPort, jarFile, configFile, mainClass);
        SubmitResponse response = JSON.parseObject(respBody, SubmitResponse.class);
        if (SubmitStatus.SUCCESS == response.getStatus()) {
            logger.info("提交任务成功,任务ID:[{}],详情:[{}]", response.getTaskId(), trimToEmpty(response.getDesc()));
        } else {
            logger.error("提交任务失败,错误信息:{}", response.getDesc());
        }
    }

    private static String submit(String serverIp, int serverPort, String jarFilePath, String configFilePath, String mainClass) throws Exception {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;

        try {
            httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost("http://" + serverIp + ":" + serverPort + "/uploadtaskfile");

            File jarFile = new File(jarFilePath);
            File configFile = new File(configFilePath);
            post.setEntity(MultipartEntityBuilder
                            .create()
                            .addBinaryBody(jarFile.getName(), jarFile)
                            .addBinaryBody(configFile.getName(), configFile)
                            .addPart(
                                    FormBodyPartBuilder.create().setName("mainClass").setBody(new StringBody(mainClass)).build()
                            )
                            .build()
            );

            response = httpClient.execute(post);

            return IOUtils.toString(response.getEntity().getContent(), "UTF-8");
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
            if (response != null) {
                response.close();
            }
        }
    }
}
