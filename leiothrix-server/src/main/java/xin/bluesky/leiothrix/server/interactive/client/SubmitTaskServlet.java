package xin.bluesky.leiothrix.server.interactive.client;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xin.bluesky.leiothrix.common.util.StringUtils2;
import xin.bluesky.leiothrix.model.bin.SubmitResponse;
import xin.bluesky.leiothrix.model.task.TaskStatus;
import xin.bluesky.leiothrix.server.action.WorkerProcessorLauncher;
import xin.bluesky.leiothrix.server.action.exception.NoResourceException;
import xin.bluesky.leiothrix.server.storage.TaskStorage;
import xin.bluesky.leiothrix.server.tablemeta.DatabaseSchemaLoader;
import xin.bluesky.leiothrix.server.util.LeiothrixUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static xin.bluesky.leiothrix.model.bin.SubmitStatus.FAIL;
import static xin.bluesky.leiothrix.model.bin.SubmitStatus.SUCCESS;

/**
 * @author 张轲
 */
public class SubmitTaskServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(SubmitTaskServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        final String taskId = UUID.randomUUID().toString();
        logger.info("接收到client[ip={}]发来的任务,为其创建任务ID:{}", request.getRemoteAddr(), taskId);
        TaskStorage.logTaskStartTime(taskId);

        DiskFileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload submit = new ServletFileUpload(factory);

        SubmitResponse submitResponse = null;
        TaskStorage.createEmptyTask(taskId, TaskStatus.ALLOCATING);

        try {
            // 任务启动前的准备工作
            prepare(request, submit, taskId);

            // 加载任务中指定的数据库表结构
            DatabaseSchemaLoader.loadDatabaseSchema(taskId);

            // 启动worker进程来处理该任务
            new WorkerProcessorLauncher(taskId).launch();

            // 返回给客户端
            submitResponse = new SubmitResponse(taskId, SUCCESS);
        } catch (NoResourceException ex) {
            //对于当前没有资源可执行的任务,已经存储下来了,等待补偿机制发现空闲资源后再触发执行
            logger.warn("当前没有可用资源来执行该任务[taskId={}]", taskId);
            submitResponse = new SubmitResponse(taskId, SUCCESS, "任务提交成功,但是当前没有可用资源,请您耐心等待,勿重复提交");
            TaskStorage.setStatus(taskId, TaskStatus.UNALLOCATED);
        } catch (Throwable e) {
            logger.error("任务[taskId={}]提交失败,错误信息:{}", taskId, ExceptionUtils.getStackTrace(e));
            submitResponse = new SubmitResponse(null, FAIL, e.getMessage());
            TaskStorage.setStatus(taskId, TaskStatus.ERROR);
            // 清理失败的任务
            clean(taskId);
        } finally {
            WebUtils.respond(response, HttpStatus.SC_OK, submitResponse);
        }

    }

    private void prepare(HttpServletRequest request, ServletFileUpload upload, String taskId) throws Exception {
        // 在服务器的文件系统上创建task目录,以保存任务相关文件
        File taskDir = LeiothrixUtils.getTaskDirPath(taskId);
        taskDir.mkdirs();

        // 读取/保存任务文件和必要参数
        List<FileItem> list = upload.parseRequest(request);
        for (FileItem item : list) {
            if (item.isFormField()) {
                handleForm(taskId, item);
            } else {
                handleStream(taskId, taskDir, item);
            }
        }

        // 把任务所需要的jar包和config发送到其他server上去,以在将来切换至其他server时,可以正常启动worker
        boolean success = TaskFileService.scp2OtherServers(taskId, taskDir);
        if (!success) {
            throw new Exception(String.format("传送任务[taskId=%s]相关文件到server时出错", taskId));
        }
    }

    private void handleForm(String taskId, FileItem item) {
        if (item.getFieldName().equals("mainClass")) {
            String mainClassValue = item.getString();

            TaskStorage.setMainClass(taskId, mainClassValue);

            logger.info("任务[{}]的mainClass为:{}", taskId, mainClassValue);
        }
    }

    private void handleStream(String taskId, File taskDir, FileItem item) throws Exception {
        String fileName = item.getName();
        String absoluteFileName = StringUtils2.append(taskDir.getAbsolutePath(), File.separator, fileName);
        item.write(new File(absoluteFileName));

        if (fileName.endsWith(".jar")) {
            TaskStorage.setJarPath(taskId, absoluteFileName);
            logger.info("任务[{}]的jar包存放路径为:{}", taskId, absoluteFileName);
        } else if (fileName.endsWith(".json")) {
            String configText = IOUtils.toString(new FileInputStream(absoluteFileName));
            TaskStorage.setConfig(taskId, configText);
            logger.info("任务[{}]的config信息为:{}", taskId, configText);
        } else {
            throw new Exception(String.format("不支持该种类型的文件[文件名:%s]", fileName));
        }
    }

    private void clean(String taskId) {
        // 删除zookeeper上该task的目录
        TaskStorage.delete(taskId);

        // 删除任务相关文件
        File taskDir = LeiothrixUtils.getTaskDirPath(taskId);
        taskDir.delete();
    }
}
