package Benchmark.Queries.Results;

/**
 * Stores the results of a Floor Total query.
 */
public class FloorTotal extends AbstractResult {
    private final int floor;
    private final String dayTimestamp;
    private final int total;

    public FloorTotal(int floor, String dayTimestamp, int total){
        this.floor = floor;
        this.dayTimestamp = dayTimestamp;
        this.total = total;
    }

    @Override
    public String print() {
        return dayTimestamp + ";" + floor + ";" + total;
    }
}
