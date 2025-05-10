package modelo;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

/**
 * Al reaparecer una réplica, reenvía el journal desde primary.
 */
public class JournalReplayer {
    static void replay(Journal journal, ServerInfo from, ServerInfo to) {
        List<String> missed = journal.tailFrom(0);
        for (String entry : missed) {
            try (Socket s = new Socket(to.ip, to.port);
                 PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
                out.println(entry);
            } catch (IOException e) {
                System.err.println("Error journal->" + to + ": " + e.getMessage());
            }
        }
    }
}