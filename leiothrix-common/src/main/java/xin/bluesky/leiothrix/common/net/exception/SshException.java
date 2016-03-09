package xin.bluesky.leiothrix.common.net.exception;

/**
 * @author 张轲
 * @date 16/1/24
 */
public class SshException extends RemoteException {
    public SshException() {
    }

    public SshException(String message) {
        super(message);
    }

    public SshException(String message, Throwable cause) {
        super(message, cause);
    }

    public SshException(Throwable cause) {
        super(cause);
    }

    public SshException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
