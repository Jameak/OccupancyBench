package Benchmark.Generator.GeneratedData;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Represents a single generated entry, ready to be added to a target.
 */
public class GeneratedEntry{
    private final String timestamp;
    private final String ap;
    private final int numClients;
    private final LocalDate date;
    private final LocalTime time;

    public GeneratedEntry(LocalDate date, LocalTime time, String AP, int numClients){
        this.date = date;
        this.time = time;
        this.timestamp = date.toString() + "T" + time.toString() + "Z";
        this.ap = AP;
        this.numClients = numClients;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getAP() {
        return ap;
    }

    public int getNumClients() {
        return numClients;
    }

    @Override
    public String toString() {
        return timestamp + ";" + ap + ";" + numClients;
    }

    public LocalDate getDate() {
        return date;
    }

    public LocalTime getTime() {
        return time;
    }
}
