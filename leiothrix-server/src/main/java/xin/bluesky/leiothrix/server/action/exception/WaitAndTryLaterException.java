package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 */
public class WaitAndTryLaterException extends Exception {
    public WaitAndTryLaterException() {
    }

    public WaitAndTryLaterException(String message) {
        super(message);
    }

    public WaitAndTryLaterException(String message, Throwable cause) {
        super(message, cause);
    }

    public WaitAndTryLaterException(Throwable cause) {
        super(cause);
    }

    public WaitAndTryLaterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
