package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 */
public class ProcessorLaunchException extends Exception{
    public ProcessorLaunchException() {
    }

    public ProcessorLaunchException(String message) {
        super(message);
    }

    public ProcessorLaunchException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessorLaunchException(Throwable cause) {
        super(cause);
    }

    public ProcessorLaunchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
