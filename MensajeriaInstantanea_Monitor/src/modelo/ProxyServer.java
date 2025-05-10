package modelo;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Monitor/Proxy que distribuye peticiones de clientes entre instancias de servidor,
 * hace health checks, reintentos y resincronización de estado mediante un journal.
 */
public class ProxyServer {
    private final int proxyPort;
    private final List<ServerInfo> servers;
    private final ScheduledExecutorService heartbeat;
    private final List<String> journal = Collections.synchronizedList(new ArrayList<>());
    //el journal solo tiene la lista de usuarios y la lista de mansajes almacenados, se pide cuando se reconecta un server

    public ProxyServer(int proxyPort, List<ServerInfo> servers) {
        this.proxyPort = proxyPort;
        this.servers = new CopyOnWriteArrayList<>(servers);
        this.heartbeat = Executors.newScheduledThreadPool(1);
    }

    public void start() throws IOException {
        // Iniciar health-check periódico
        heartbeat.scheduleAtFixedRate(this::checkServers, 0, 5, TimeUnit.SECONDS);

        // Iniciar proxy listener
        try (ServerSocket proxySocket = new ServerSocket(proxyPort)) {
            System.out.println("Proxy escuchando en puerto " + proxyPort);
            while (true) {
                Socket clientSocket = proxySocket.accept();
                new ClientHandler(clientSocket).start();
            }
        }
    }

    private void checkServers() {
        for (ServerInfo s : servers) {
            boolean alive = s.ping();
            if (alive && !s.wasAlive()) {
                // servidor acaba de volver: resincronizar
                replayJournalTo(s);
            }
            s.setAlive(alive);
        }
    }

    private void replayJournalTo(ServerInfo server) {
        System.out.println("Resincronizando servidor " + server);
        synchronized (journal) {
            for (String req : journal) {
                try (Socket sock = new Socket(server.ip, server.port);
                     PrintWriter sout = new PrintWriter(sock.getOutputStream(), true)) {
                    sout.println(req);
                } catch (IOException e) {
                    System.err.println("Error al resincronizar a " + server + ": " + e.getMessage());
                    break;
                }
            }
        }
    }

    private class ClientHandler extends Thread {
        private final Socket client;
        public ClientHandler(Socket client) { this.client = client; }

        @Override
        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream(), true)
            ) {
                String request = in.readLine();
                if (request == null) return;

                // Guardar en journal para resincronización futura
                //esto no va, el journal debe tener solo la info que me envia el servidor cuando se la pido
                journal.add(request);

                // Fan-out a servidores activos con reintentos
                String response = forwardToServers(request);
                out.println(response);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try { client.close(); } catch (IOException ignored) {}
            }
        }

        private String forwardToServers(String req) {
            for (ServerInfo s : servers) {
                if (!s.isAlive()) continue;
                for (int i = 0; i < 3; i++) {
                    try (Socket sock = new Socket(s.ip, s.port);
                         PrintWriter sout = new PrintWriter(sock.getOutputStream(), true);
                         BufferedReader sin = new BufferedReader(new InputStreamReader(sock.getInputStream()))) {
                        sout.println(req);
                        return sin.readLine();
                    } catch (IOException e) {
                        // retry
                    }
                }
            }
            return "ERROR: No hay servidores disponibles";
        }
    }
    
    // metodo para pedir la info a un server activo
    

    // Información de cada réplica
    public static class ServerInfo {
        public final String ip;
        public final int port;
        private volatile boolean alive;

        public ServerInfo(String ip, int port) {
            this.ip = ip;
            this.port = port;
            this.alive = false;
        }

        public boolean ping() {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(ip, port), 2000);
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        public boolean isAlive() { return alive; }
        public void setAlive(boolean a) { this.alive = a; }
        public boolean wasAlive() { return alive; } // track previous state
        public String toString() { return ip + ":" + port; }
    }

    // Método main de ejemplo
    public static void main(String[] args) throws IOException {
        List<ServerInfo> lista = Arrays.asList(
            new ServerInfo("localhost", 1234),
            new ServerInfo("localhost", 1235),
            new ServerInfo("localhost", 1236)
        );
        new ProxyServer(9000, lista).start();
    }
}
