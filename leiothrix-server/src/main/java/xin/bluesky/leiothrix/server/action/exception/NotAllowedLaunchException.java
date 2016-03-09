package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 * @date 16/2/18
 */
public class NotAllowedLaunchException extends Exception {
    public NotAllowedLaunchException() {
    }

    public NotAllowedLaunchException(String message) {
        super(message);
    }

    public NotAllowedLaunchException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotAllowedLaunchException(Throwable cause) {
        super(cause);
    }
}
