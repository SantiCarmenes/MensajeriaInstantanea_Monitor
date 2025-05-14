package modelo;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class ProxyServer {
    private static final int PORT = 60000;
    private final List<ServerConnection> servers = new CopyOnWriteArrayList<>();
    private int nextIndex = -1;

    public static void main(String[] args) throws IOException {
        new ProxyServer().start();
    }

    public void start() throws IOException {
        try (ServerSocket listener = new ServerSocket(PORT)) {
            System.out.println("Proxy listening on port " + PORT);
            while (true) {
                Socket sock = listener.accept();
                new Thread(() -> handleConnection(sock)).start();
            }
        }
    }

    private void handleConnection(Socket sock) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true)) {

            String header = in.readLine();
            if (header == null) return;
            String op = parseField(header, "OPERACION");

            switch (op) {
                case "REGISTER":
                    registerServer(header, in, out);
                    break;
                case "RECONNECT_USER":
                    reconnectUser(header, in, out);
                    break;
                default:
                    forwardClient(header, in, out);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void registerServer(String header, BufferedReader in, PrintWriter out) throws IOException {
        String ip = parseField(header, "IP");
        int port = Integer.parseInt(parseField(header, "PUERTO"));
        ServerConnection sc = new ServerConnection(ip, port);
        servers.add(sc);
        out.println("RESPUESTA:ACK");
        // synchronize state if needed
    }

    private void reconnectUser(String header, BufferedReader in, PrintWriter out) throws IOException {
        String user = parseField(header, "USER");
        // retrieve pending messages from proxy journal
        String pending = getPendingFor(user);
        out.println("RESPUESTA:" + pending);
        // propagate cleanup to servers
    }

    private void forwardClient(String header, BufferedReader in, PrintWriter out) throws IOException {
        String body = in.readLine();
        String fullReq = header + "\n" + body;
        String resp = forwardWithRetry(fullReq);
        if (resp != null) out.println(resp);
        else out.println("ERROR;NO_BACKEND");
    }

    private String forwardWithRetry(String req) {
        for (int i = 0; i < 3; i++) {
            ServerConnection sc = pickNextAlive();
            if (sc == null) break;
            try {
                String r = sc.sendAndReceive(req);
                if ("ACK".equalsIgnoreCase(parseField(r, "RESPUESTA"))) return r;
            } catch (IOException e) {
                sc.markDead();
            }
        }
        return null;
    }

    private ServerConnection pickNextAlive() {
        int n = servers.size();
        for (int i = 0; i < n; i++) {
            nextIndex = (nextIndex + 1) % n;
            ServerConnection sc = servers.get(nextIndex);
            if (sc.isAlive()) return sc;
        }
        return null;
    }

    private String parseField(String line, String key) {
        String[] parts = line.split(";");
        for (String p: parts) {
            if (p.startsWith(key + ":")) return p.substring((key + ":").length());
        }
        return "";
    }

    private String getPendingFor(String user) {
        // TODO: look up journal map
        return "";
    }
}