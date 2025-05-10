package modelo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Journal en memoria (puede persistir en fichero).
 */
public class Journal {
    private final List<String> entries = Collections.synchronizedList(new ArrayList<>());
    void append(String req) { entries.add(req); }
    List<String> tailFrom(int offset) {
        synchronized (entries) {
            return new ArrayList<>(entries.subList(offset, entries.size()));
        }
    }
}