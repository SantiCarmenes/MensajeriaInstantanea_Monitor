package modelo;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ProxyServer: escucha en un único puerto (60000),
 * acepta conexiones de clientes y servidores,
 * enruta peticiones a un backend vivo usando round‑robin,
 * reintenta hasta 3 veces con ACK antes de cambiar de servidor,
 * y mantiene un heartbeat periódico para detectar réplicas caídas/recuperadas.
 */
public class ProxyServer {
    public static final int PROXY_PORT = 60000;
    private final ServerSocket listenSocket;
    private final List<ServerConnection> backends = new CopyOnWriteArrayList<>();
    private final AtomicInteger rrCounter = new AtomicInteger(0);
    private final ScheduledExecutorService heartbeatExec = Executors.newSingleThreadScheduledExecutor();

    public ProxyServer() throws IOException {
        this.listenSocket = new ServerSocket(PROXY_PORT);
        // Arrancamos heartbeat: cada 5s ping a todos los backends registrados
        heartbeatExec.scheduleAtFixedRate(this::runHeartbeat, 5, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("[PROXY] Arrancando proxy en puerto " + PROXY_PORT);
        new ProxyServer().acceptLoop();
    }

    /** Bucle principal: acepta conexiones entrantes (clientes y servidores). */
    private void acceptLoop() {
        while (true) {
            try {
                Socket sock = listenSocket.accept();
                // Cada conexión la manejamos en un hilo aparte
                new Thread(() -> handleConnection(sock)).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** Lee la primera línea para decidir la operación y despacha. */
    private void handleConnection(Socket sock) {
        try (
            BufferedReader in  = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            PrintWriter    out = new PrintWriter(sock.getOutputStream(), true)
        ) {
            String header = in.readLine();
            if (header == null) return;

            String op = parseField(header, "OPERACION");
            switch (op) {
                case "REGISTER":      registerBackend(header, out);       break;
                case "CLIENT_REQ":    forwardClient(header, in, out);     break;
                default:
                    out.println("ERROR;MSG:Operacion desconocida");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try { sock.close(); } catch (IOException ignored) {}
        }
    }

    //––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

    /**  
     * REGISTRO DE UN BACKEND.  
     * HEADER ejemplo: "OPERACION:REGISTER;IP:1.2.3.4;PUERTO:12345"
     */
    private void registerBackend(String header, PrintWriter out) {
        String ip   = parseField(header, "IP");
        int    port = Integer.parseInt(parseField(header, "PUERTO"));
        ServerConnection sc = new ServerConnection(ip, port);
        backends.add(sc);
        out.println("RESPUESTA:ACK");
        System.out.println("[PROXY] Backend registrado: " + ip + ":" + port);
    }

    /**
     * Reenvía la petición de cliente al backend vivo seleccionado.
     * Retries + espera de ACK.  
     * HEADER ejemplo: "OPERACION:CLIENT_REQ;USER:Foo"
     * BODY: la línea siguiente (por ejemplo, JSON u otro formato).
     */
    private void forwardClient(String header, BufferedReader in, PrintWriter out) throws IOException {
        String body  = in.readLine();
        String full  = header + "\n" + (body == null ? "" : body);
        String reply = forwardWithRetry(full);
        out.println(reply);
    }

    /**
     * Round‑Robin + retry hasta 3 veces esperando un ACK en formato "ACK".
     * Si no llega, marca el backend muerto y continúa con el siguiente.
     */
    private String forwardWithRetry(String req) {
        int backendCount = backends.size();
        if (backendCount == 0) return "ERROR;MSG:No hay backends disponibles";

        // Intentamos como máximo `backends.size()` distintos servidores
        for (int i = 0; i < backendCount; i++) {
            int idx = rrCounter.getAndIncrement() % backendCount;
            ServerConnection sc = backends.get(idx);
            if (!sc.isAlive()) continue;

            try {
                // sendAndAwaitAck reintenta internamente 3 veces
                String resp = sc.sendAndAwaitAck(req);
                return resp;
            } catch (IOException e) {
                System.err.println("[PROXY] Falló backend " + sc + ", lo marcamos muerto.");
                sc.markDead();
                // seguimos al siguiente
            }
        }
        return "ERROR;MSG:Todos los backends caidos";
    }

    /** Cada 5s hacemos ping a todos los backends para mantener vivos/muertos */
    private void runHeartbeat() {
        for (ServerConnection sc : backends) {
            if (!sc.checkHeartbeat()) {
                System.err.println("[PROXY] Backend no responde: " + sc);
                sc.markDead();
            } else {
                sc.markAlive();
            }
        }
    }

    /** Extrae valor tras "clave:valor" en un header separado por ';' */
    private String parseField(String header, String key) {
        for (String part : header.split(";")) {
            if (part.startsWith(key + ":")) {
                return part.substring(key.length()+1).trim();
            }
        }
        return "";
    }
}
