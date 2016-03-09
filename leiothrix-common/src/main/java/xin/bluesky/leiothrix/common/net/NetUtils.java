package xin.bluesky.leiothrix.common.net;

import xin.bluesky.leiothrix.common.net.exception.PingException;
import xin.bluesky.leiothrix.common.net.exception.SshException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.regex.Pattern;

/**
 * @author 张轲
 * worker.processor.threadnum.factor
 */
public class NetUtils {

    private static Pattern pattern = Pattern.compile("[1-9]{3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");

    public static String getLocalIp() {
        String address = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        address = addresses.nextElement().getHostAddress();
                        if (pattern.matcher(address).matches()
                                && !address.trim().startsWith("127.0.0.1")
                                && !address.trim().startsWith("172.17.42")) {
                            return address;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (address == null) {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        return address;
    }

    public static boolean pingSuccess(String remoteIp) {
        try {
            return InetAddress.getByName(remoteIp).isReachable(3000);
        } catch (IOException e) {
            throw new PingException(String.format("ping远程主机[%s]时发生异常", remoteIp));
        }
    }

    public static boolean sshSuccess(String remoteIp, String username) {
        String command = String.format("ssh -l %s %s %s", username, remoteIp, "echo 1");
        try {
            int exitValue = Runtime.getRuntime().exec(command).waitFor();
            return exitValue == 0;
        } catch (InterruptedException | IOException e) {
            throw new SshException(String.format("无密ssh到远程主机[ip=%s,username=%]失败,请检查是否配置了无密登陆", remoteIp, username));
        }
    }
}
