package xin.bluesky.leiothrix.model.msg;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by zhangke on 15/9/1.
 */
public class WorkerMessage implements Serializable {

    private String type;
    private String ip;
    private long timestamp;
    protected String data;

    public WorkerMessage() {
    }

    public WorkerMessage(String type, String data, String ip) {
        this.data = data;
        this.type = type;
        this.ip = ip;
        this.timestamp = new Date().getTime();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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
        return "Message{" +
                "type='" + type + '\'' +
                ", ip='" + ip + '\'' +
                ", timestamp=" + timestamp +
                ", data='" + data + '\'' +
                '}';
    }

}
