package xin.bluesky.leiothrix.common.net.exception;

/**
 * @author 张轲
 * worker.processor.threadnum.factor
 */
public class PingException extends RemoteException {
    public PingException() {
    }

    public PingException(String message) {
        super(message);
    }

    public PingException(String message, Throwable cause) {
        super(message, cause);
    }

    public PingException(Throwable cause) {
        super(cause);
    }

    public PingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
