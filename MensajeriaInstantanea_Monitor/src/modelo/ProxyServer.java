package modelo;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * ProxyServer actúa como monitor y equilibrador de carga entre réplicas de Servidor.
 * Implementa redundancia activa, heartbeat, retry con ACK y journal para resincronización.
 */
public class ProxyServer {
    private final int proxyPort;
    private final int registrationPort;
    private final List<ServerInfo> servers = Collections.synchronizedList(new ArrayList<>());
    private volatile int primaryIndex = 0;
    private final ScheduledExecutorService hbExec;
    private final Journal journal;

    public ProxyServer(int proxyPort, int registrationPort) { //atiende clientes en 65000 y registros en 65001
        this.proxyPort = proxyPort;
        this.registrationPort = registrationPort;
        this.hbExec = Executors.newSingleThreadScheduledExecutor();
        this.journal = new Journal();
    }

    public void start() throws IOException {
        // Inicia heartbeat periódico
        hbExec.scheduleAtFixedRate(this::heartbeat, 0, 5, TimeUnit.SECONDS);

        // Arranca hilo de registro de servidores
        new Thread(this::handleRegistrations).start();

        // Escucha conexiones de clientes
        try (ServerSocket ss = new ServerSocket(proxyPort)) {
            System.out.println("Proxy escuchando en puerto cliente " + proxyPort);
            while (true) {
                Socket client = ss.accept();
                new ProxyHandler(client).start();
            }
        }
    }

    /** Atención de peticiones de registro de servidores */
    private void handleRegistrations() {
        try (ServerSocket regSock = new ServerSocket(registrationPort)) {
            System.out.println("Proxy escuchando registros en puerto " + registrationPort);
            while (true) {
                Socket sock = regSock.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String line = in.readLine(); // espera "REGISTER ip:port"
                if (line != null && line.startsWith("REGISTER ")) {
                    String[] parts = line.substring(9).split(":");
                    String ip = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    ServerInfo srv = new ServerInfo(ip, port);
                    servers.add(srv);
                    System.out.println("Servidor registrado: " + srv);
                }
                sock.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Verifica salud de réplicas y resincroniza nuevas */
    private void heartbeat() {
        synchronized (servers) {
            for (int i = 0; i < servers.size(); i++) {
                ServerInfo srv = servers.get(i);
                boolean alive = srv.ping();
                if (!alive && primaryIndex == i) {
                    primaryIndex = (i + 1) % servers.size();
                    System.out.println("Failover a " + servers.get(primaryIndex));
                }
                if (alive && i != primaryIndex && !srv.handshaken) {
                    JournalReplayer.replay(journal, servers.get(primaryIndex), srv);
                    srv.handshaken = true;
                    System.out.println("Journal sync a " + srv);
                }
            }
        }
    }

    /** Envía con retry+ACK y failover; devuelve respuesta al cliente */
    private String forwardWithRetry(String req) {
        journal.append(req);
        int start = primaryIndex;
        for (int offset = 0; offset < servers.size(); offset++) {
            int idx = (start + offset) % servers.size();
            ServerInfo srv = servers.get(idx);
            if (!srv.isAlive()) continue;
            for (int attempt = 1; attempt <= 3; attempt++) {
                try (Socket sock = new Socket()) {
                    sock.connect(new InetSocketAddress(srv.ip, srv.port), 2000);
                    sock.setSoTimeout(2000);
                    PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    out.println(req);
                    String ack = in.readLine();
                    if ("OK".equals(ack)) {
                        out.println("GIVE_RESPONSE");
                        return in.readLine();
                    }
                } catch (IOException e) {
                    // retry
                }
            }
            // marcar muerto y cambiar primary
            srv.setAlive(false);
            primaryIndex = (idx + 1) % servers.size();
            System.err.println("Servidor " + srv + " no responde; conmutando.");
        }
        return "ERROR: ningún servidor disponible";
    }

    /** Handler que atiende a cada cliente */
    private class ProxyHandler extends Thread {
        private final Socket client;
        ProxyHandler(Socket client) { this.client = client; }
        @Override
        public void run() {
            try (
                BufferedReader inCl = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter outCl = new PrintWriter(client.getOutputStream(), true)
            ) {
                String req = inCl.readLine();
                if (req == null) return;
                String resp = forwardWithRetry(req);
                outCl.println(resp);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try { 
                	client.close(); 
                } catch (IOException ignored) {}
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new ProxyServer(65000, 65001).start();
    }
}