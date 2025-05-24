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
    private final HashMap<String,Socket> connectedUsersMap = new HashMap<String, Socket>();

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
        	String header;
        	while((header = in.readLine()) != null) {        		
        		//if (header == null) return;
        		
        		System.out.println("[PROXY] Conectado desde "+sock.getInetAddress() + ":" + sock.getPort());
        		String op = parseField(header, "OPERACION");
        		System.out.println(header);
        		String address;
        		switch (op) {
        		case "REGISTER":
        			registerBackend(header, out);
        			sock.close();
        			break;
        		case "CLIENT_REQ":
        			forwardClient(header.concat(";ADDRESS:"+sock.getInetAddress()+sock.getPort()), in, out);
        			address = parseField(header,"ADDRESS");
        			if(address.equals("")) {
        				address = sock.getInetAddress()+""+sock.getPort();
        				this.connectedUsersMap.putIfAbsent(address, sock);
        				System.out.println("Conexion registrada");
        			}
        			break;
        		case "MESSAGE":
        			forwardClient(header, in, out);
        			break;
        		case "SEND_MESSAGE":
        			sendServerMessage(header,in,out);
        			break;
        		default:
        			out.println("ERROR;MSG:Operacion desconocida");
        		}
        	}
        } catch (IOException ioe) {
        	String address = sock.getInetAddress()+ "" +sock.getPort();
        	if(this.connectedUsersMap.get(address) != null) {
        		this.notifyClientDisconnected(address);
        		System.out.println("eliminando usuario desconectado");
        		this.connectedUsersMap.remove(address);
        	}
        	System.out.println("[PROXY] Desconectado: " + sock.getInetAddress()+":"+sock.getPort());
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
        
        System.out.println(reply);
        out.println("OPERACION:RESPUESTA");
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
    
    private void notifyClientDisconnected(String address) {
    	String header = "OPERACION:DISCONNECT;ADDRESS:" + address;
    	String full  = header + "\n";
    	
    	String reply = this.forwardWithRetry(full);
    	
    	System.out.println("Respuesta servidor" + reply);
    	
    }
    
    private void sendServerMessage(String header,BufferedReader in,PrintWriter out) throws IOException{
    	String body  = in.readLine();
    	String address = parseField(header,"ADDRESS");
    	String response;
    	Socket socket = connectedUsersMap.get(address);
    	BufferedReader inClient;
    	PrintWriter outClient;
    	if(socket != null) {
    		inClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    		outClient = new PrintWriter(socket.getOutputStream(), true);
    		outClient.println("OPERACION:GET_MESSAGE");
    		outClient.println(body);
    		out.println("ACK");
    	}else{
    		out.println("OPERACION:RESEND_ERROR");
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
