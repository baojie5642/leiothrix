package xin.bluesky.leiothrix.server.action.exception;

/**
 * @author 张轲
 */
public class NoResourceException extends Exception{
    public NoResourceException() {
    }

    public NoResourceException(String message) {
        super(message);
    }

    public NoResourceException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoResourceException(Throwable cause) {
        super(cause);
    }
}
