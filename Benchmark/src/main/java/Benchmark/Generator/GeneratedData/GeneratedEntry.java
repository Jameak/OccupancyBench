package Benchmark.Generator.GeneratedData;

import Benchmark.Config.ConfigFile;

import java.time.*;

/**
 * Represents a single generated entry, ready to be added to a target.
 */
public class GeneratedEntry{
    private final String ap;
    private final int numClients;
    private final LocalDateTime datetime;
    private final Instant instant;

    public GeneratedEntry(LocalDate date, LocalTime time, String AP, int numClients){
        this.datetime = date.atTime(time);
        this.instant = datetime.toInstant(ZoneOffset.ofHours(0));
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

    public LocalDateTime getDateTime() {
        return datetime;
    }

    public long getTime(ConfigFile.Granularity granularity){
        switch (granularity){
            case NANOSECOND:
                return instant.getEpochSecond() * 1_000_000_000 + datetime.getNano();
            case MILLISECOND:
                return instant.toEpochMilli();
            case SECOND:
                return instant.getEpochSecond();
            case MINUTE:
                return instant.getEpochSecond() / 60;
            default:
                throw new IllegalStateException("Unexpected value: " + granularity);
        }
    }
}
