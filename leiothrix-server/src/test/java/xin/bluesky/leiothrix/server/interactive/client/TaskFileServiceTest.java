package xin.bluesky.leiothrix.server.interactive.client;

import org.junit.Ignore;
import org.junit.Test;
import xin.bluesky.leiothrix.server.conf.ServerConfigure;
import xin.bluesky.leiothrix.server.storage.ServerStorage;
import xin.bluesky.leiothrix.server.support.StorageContainerDependency;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author 张轲
 * @date 16/3/7
 */
@Ignore(value = "会依赖外部可以ssh通的centos,所以忽略")
public class TaskFileServiceTest extends StorageContainerDependency {

    @Test
    public void should_scp_correct() throws Exception {
        // 拷贝一个任务的文件目录
        final String localhost = "192.168.10.231";
        ServerStorage.registerServer(localhost);
        ServerConfigure.setProperty("server", localhost);

        ServerStorage.registerServer("192.168.5.105");//另外一个server

        String taskId = UUID.randomUUID().toString();
        System.out.println(taskId);
        File taskDir = createTaskDirectory(taskId);

        // when
        boolean result = TaskFileService.scp2OtherServers(taskId, taskDir);

        // then
        assertThat(result, is(true));

        //删除该任务的文件目录
        TaskFileService.deleteOnAllServers(taskId);
    }

    private File createTaskDirectory(String taskId) throws IOException {
        File file = new File("/tmp/" + taskId);
        file.deleteOnExit();
        if (!file.exists()) {
            file.mkdirs();
        }
        File jarFile = new File(file.getAbsolutePath() + "/application.jar");
        jarFile.createNewFile();
        File configFile = new File(file.getAbsolutePath() + "/config.properties");
        configFile.createNewFile();

        return file;
    }
}