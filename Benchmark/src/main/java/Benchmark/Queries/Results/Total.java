package Benchmark.Queries.Results;

/**
 * Stores the results of a TotalClients query.
 */
public class Total extends AbstractResult {
    private final String dayTimestamp;
    private final int total;

    public Total(String dayTimestamp, int total){
        this.dayTimestamp = dayTimestamp;
        this.total = total;
    }

    @Override
    public String print() {
        return dayTimestamp + ";" + total;
    }
}
