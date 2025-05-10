package modelo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Información de cada réplica: dirección, puerto, estado y flag de sync.
 */
public class ServerInfo {
    final String ip;
    final int port;
    volatile boolean alive = true;
    boolean handshaken = false;
    ServerInfo(String ip, int port) { this.ip = ip; this.port = port; }
    boolean ping() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(ip, port), 1000);
            return true;
        } catch (IOException e) {
            alive = false;
            return false;
        }
    }
    void setAlive(boolean v) { alive = v; }
    boolean isAlive() { return alive; }
    public String toString() { return ip + ":" + port; }
}
