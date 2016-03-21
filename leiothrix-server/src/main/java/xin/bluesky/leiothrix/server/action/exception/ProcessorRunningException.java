package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 */
public class ProcessorRunningException extends Exception {
    public ProcessorRunningException() {
    }

    public ProcessorRunningException(String message) {
        super(message);
    }

    public ProcessorRunningException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessorRunningException(Throwable cause) {
        super(cause);
    }

    public ProcessorRunningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
