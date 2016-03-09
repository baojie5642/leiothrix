package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class NoTaskException extends Exception {
    public NoTaskException() {
    }

    public NoTaskException(String message) {
        super(message);
    }

    public NoTaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoTaskException(Throwable cause) {
        super(cause);
    }

    public NoTaskException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
