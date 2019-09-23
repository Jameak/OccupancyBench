package Generator;

import java.util.Map;

public class Entry{
    private final String time;
    private final int total;
    private final Map<Integer, Double> probabilities;

    public Entry(String time, int total, Map<Integer, Double> probabilities){
        this.time = time;
        this.total = total;
        this.probabilities = probabilities == null || probabilities.isEmpty() ? null : probabilities;
    }

    public String getTime() {
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