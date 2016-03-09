package xin.bluesky.leiothrix.server.conf;

/**
 * @author 张轲
 * @date 16/1/19
 */
public class ConfigureException extends RuntimeException {
    public ConfigureException() {
    }

    public ConfigureException(String message) {
        super(message);
    }

    public ConfigureException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigureException(Throwable cause) {
        super(cause);
    }

    public ConfigureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
