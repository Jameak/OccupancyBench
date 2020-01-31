package Benchmark.Generator.GeneratedData;

import Benchmark.Config.ConfigFile;

import java.time.*;

/**
 * Represents a single generated entry for the row-based schema, ready to be added to a target.
 */
public class GeneratedRowEntry extends AbstractGeneratedEntry {
    private final String ap;
    private final int numClients;

    public GeneratedRowEntry(LocalDate date, LocalTime time, String AP, int numClients){
        super(date, time);
        this.ap = AP;
        this.numClients = numClients;
    }

    public String getAP() {
        return ap;
    }

    public int getNumClients() {
        return numClients;
    }

    @Override
    public String toString() {
        return toString(ConfigFile.Granularity.NANOSECOND);
    }

    public String toString(ConfigFile.Granularity granularity) {
        return getTime(granularity) + ";" + ap + ";" + numClients;
    }
}
