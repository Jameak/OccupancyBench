package Benchmark.SeedLoader.Seeddata;

import java.time.LocalDate;
import java.util.Map;

public class SeedEntries {
    public final Map<LocalDate, Entry[]> loadedEntries;

    public SeedEntries(Map<LocalDate, Entry[]> loadedEntries) {
        this.loadedEntries = loadedEntries;
    }
}
