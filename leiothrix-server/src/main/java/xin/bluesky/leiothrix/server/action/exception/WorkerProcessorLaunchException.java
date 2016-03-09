package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 * @date 16/1/20
 */
public class WorkerProcessorLaunchException extends Exception{
    public WorkerProcessorLaunchException() {
    }

    public WorkerProcessorLaunchException(String message) {
        super(message);
    }

    public WorkerProcessorLaunchException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkerProcessorLaunchException(Throwable cause) {
        super(cause);
    }

    public WorkerProcessorLaunchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
