package xin.bluesky.leiothrix.worker.client;

import io.netty.channel.pool.SimpleChannelPool;

import static xin.bluesky.leiothrix.worker.client.ChannelStatus.NORMAL;

/**
 * @author 张轲
 * @date 16/2/16
 */
public class ChannelPool {

    private String ip;

    private SimpleChannelPool channelPool;

    private ChannelStatus channelStatus = NORMAL;

    public ChannelPool() {
    }

    public ChannelPool(String ip, SimpleChannelPool channelPool) {
        this.ip = ip;
        this.channelPool = channelPool;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public SimpleChannelPool getChannelPool() {
        return channelPool;
    }

    public void setChannelPool(SimpleChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    public ChannelStatus getChannelStatus() {
        return channelStatus;
    }

    public void setChannelStatus(ChannelStatus channelStatus) {
        this.channelStatus = channelStatus;
    }

    @Override
    public String toString() {
        return this.ip;
    }
}
