package Benchmark.Queries.Results;

/**
 * Abstract class for query-results, mandating that the query-results can be pretty-printed.
 */
public abstract class AbstractResult {
    public abstract String print();

    @Override
    public final String toString() {
        return print();
    }
}
