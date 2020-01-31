package Benchmark.Generator.GeneratedData;

import java.time.*;
import java.util.HashMap;

/**
 * Represents a single generated entry for the column-based schema.
 */
public class GeneratedColumnEntry extends AbstractGeneratedEntry {
    // NOTE: Some APs may be missing from this map
    private final HashMap<String, Integer> apToNumClientsMap;

    public GeneratedColumnEntry(LocalDate date, LocalTime time, HashMap<String, Integer> apToNumClientsMap){
        super(date, time);
        this.apToNumClientsMap = apToNumClientsMap;
    }

    public HashMap<String, Integer> getMapping(){
        return apToNumClientsMap;
    }
}
