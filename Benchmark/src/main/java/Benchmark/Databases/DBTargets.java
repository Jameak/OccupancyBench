package Benchmark.Databases;

/**
 * The databases supported by the benchmark.
 */
public enum DBTargets{
    /**
     * Generated data can be written straight to a csv file where entries are separated by semicolons.
     * Querying these csv-files isn't supported.
     */
    CSV,
    /**
     * Writing generated data to InfluxDB and querying it afterward is supported by the benchmark.
     * The benchmark was tested with Influx v1.6.1
     */
    INFLUX,
    /**
     * Writing generated data to TimescaleDB and querying it afterward is supported by the benchmark.
     * The benchmark was tested with Timescale v1.4.0 running on PostgreSQL 11.4
     */
    TIMESCALE
}
