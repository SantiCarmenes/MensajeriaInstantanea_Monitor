package modelo;

import java.io.*;
import java.net.*;
import java.util.concurrent.TimeUnit;

/**
 * Representa una réplica de servidor backend:
 *  - Mantiene IP/puerto
 *  - checkHeartbeat(): un simple TCP connect+close
 *  - sendAndAwaitAck(): envía la petición, espera "ACK" como primera línea
 *      y luego retorna la respuesta que venga a continuación.
 */
public class ServerConnection {
    final String ip;
    final int    port;
    private volatile boolean alive = true;

    public ServerConnection(String ip, int port) {
        this.ip   = ip;
        this.port = port;
    }

    /** Intento de ping: abrimos+cerramos socket en timeout corto */
    public boolean checkHeartbeat() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(ip, port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** Marca esta réplica como caída */
    public void markDead()  { alive = false; }
    /** Marca como viva (tras heartbeat OK) */
    public void markAlive() { alive = true; }
    public boolean isAlive(){ return alive; }

    @Override
    public String toString() {
        return ip + ":" + port;
    }

    /**
     * Envía `req` y espera:
     *   1) primera línea "ACK"
     *   2) luego lee la segunda línea como la respuesta efectiva
     * Internamente reintenta HASTA 3 veces si no recibe ACK en ≤1s.
     */
    public String sendAndAwaitAck(String req) throws IOException {
        IOException lastEx = null;

        for (int attempt = 1; attempt <= 3; attempt++) {
            try (
                Socket s  = new Socket();
            ) {
                s.connect(new InetSocketAddress(ip, port), 1000);
                s.setSoTimeout(1000);
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));

                // 1) Enviar petición
                out.println(req);

                // 2) Esperar ACK
                String line = in.readLine();
                System.out.println(line);
                if (!"ACK".equalsIgnoreCase(line)) {
                    throw new IOException("No vino ACK (vino: " + line + ")");
                }
                // 3) Leer respuesta real
                String resp = in.readLine();
                System.out.println("respuesta " + resp);
                return resp != null ? resp : "";

            } catch (IOException ioe) {
                lastEx = ioe;
                // pequeña espera antes del retry
                try { TimeUnit.MILLISECONDS.sleep(200); } catch (InterruptedException ignored) {}
            }
        }

        // si agotamos retries, lanzamos
        throw new IOException("No ACK tras 3 intentos", lastEx);
    }
}
