package xin.bluesky.leiothrix.model.msg;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by zhangke on 15/9/1.
 */
public class ServerMessage implements Serializable {

    private String type;
    private long timestamp;
    protected String data;

    public ServerMessage() {
    }

    public ServerMessage(String type, String data) {
        this.data = data;
        this.type = type;
        this.timestamp = new Date().getTime();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "ServerMessage{" +
                "type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", data='" + data + '\'' +
                '}';
    }
}
