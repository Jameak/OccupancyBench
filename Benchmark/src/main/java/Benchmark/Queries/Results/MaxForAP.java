package Benchmark.Queries.Results;

/**
 * Stores the results of a Max For AP query.
 */
public class MaxForAP extends AbstractResult {
    private final String AP;
    private final String time;
    private final int maxVal;

    public MaxForAP(String AP, String time, int maxVal){
        this.AP = AP;
        this.time = time;
        this.maxVal = maxVal;
    }

    @Override
    public String print() {
        return AP + ";" + time + ";" + maxVal;
    }
}
