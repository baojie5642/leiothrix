package xin.bluesky.leiothrix.server.tablemeta;

/**
 * @author 张轲
 */
public class DataLoaderException extends RuntimeException{
    public DataLoaderException() {
    }

    public DataLoaderException(String message) {
        super(message);
    }

    public DataLoaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataLoaderException(Throwable cause) {
        super(cause);
    }

    public DataLoaderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
