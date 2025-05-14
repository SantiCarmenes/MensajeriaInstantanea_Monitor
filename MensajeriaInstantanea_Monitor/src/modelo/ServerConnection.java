package modelo;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ServerConnection {
    final String ip;
    final int port;
    private Socket sock;
    private BufferedReader in;
    private PrintWriter out;
    private volatile boolean alive;
    private final ScheduledExecutorService hb;

    public ServerConnection(String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        connect();
        hb = Executors.newSingleThreadScheduledExecutor();
        hb.scheduleAtFixedRate(this::heartbeat, 5, 5, TimeUnit.SECONDS);
    }

    private synchronized void connect() throws IOException {
        sock = new Socket(ip, port);
        in  = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        out = new PrintWriter(sock.getOutputStream(), true);
        alive = true;
    }

    private void heartbeat() {
        try {
            synchronized(this) {
                out.println("OPERACION:PING");
                sock.setSoTimeout(3000);
                String r = in.readLine();
                if (!"PONG".equalsIgnoreCase(r)) throw new IOException();
            }
        } catch (IOException e) {
            alive = false;
            try { sock.close(); } catch (IOException ignore) {}
            try { connect(); }
            catch (IOException ex) { /* still down */ }
        }
    }

    public synchronized String sendAndReceive(String msg) throws IOException {
        out.println(msg);
        return in.readLine();
    }

    public boolean isAlive() {
        return alive && sock != null && sock.isConnected();
    }

    public void markDead() {
        alive = false;
    }

    public void close() {
        hb.shutdownNow();
        try { sock.close(); } catch(IOException ignored) {}
    }
}
