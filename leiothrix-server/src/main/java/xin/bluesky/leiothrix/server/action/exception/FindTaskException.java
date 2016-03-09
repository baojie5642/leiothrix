package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 */
public class FindTaskException extends RuntimeException {
    public FindTaskException() {
    }

    public FindTaskException(String message) {
        super(message);
    }

    public FindTaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public FindTaskException(Throwable cause) {
        super(cause);
    }

    public FindTaskException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
