package Benchmark.Loader;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Represents a single point in time loaded from the source data.
 *
 * Holds a mapping from every access-point ID that is present in the data at that point in time,
 * to the density of the population at each of those access points at the represented time.
 *
 * This density is represented by a percentage of the total that is connected to each specific AP.
 */
public class Entry{
    private final LocalDateTime time;
    private final int total;
    private final Map<Integer, Double> probabilities;

    public Entry(LocalDateTime time, int total, Map<Integer, Double> probabilities){
        this.time = time;
        this.total = total;
        this.probabilities = probabilities == null || probabilities.isEmpty() ? null : probabilities;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public int getTotal() {
        return total;
    }

    public Map<Integer, Double> getProbabilities() {
        return probabilities;
    }

    public boolean hasData(){
        return probabilities != null;
    }
}