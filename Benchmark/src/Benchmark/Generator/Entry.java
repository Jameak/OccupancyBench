package Benchmark.Generator;

import java.time.LocalDateTime;
import java.util.Map;

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