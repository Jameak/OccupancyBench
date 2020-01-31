package Benchmark.Databases;

/**
 * The schema-formats supported by the benchmark.
 */
public enum SchemaFormats {
    /**
     * Row-based table format with sample data:
     *
     * time                | AP       | clients
     * ----------------------------------------
     * 2020-01-01 10:15:00 | AP-name1 | 5
     * 2020-01-01 10:15:00 | AP-name2 | 10
     * 2020-01-01 10:15:00 | AP-name3 | 15
     */
    ROW,
    /**
     * Column-based table format with sample data:
     *
     * time                | AP-name1 | AP-name2 | AP-name3
     * ----------------------------------------------------
     * 2020-01-01 10:15:00 | 5        | 10       | 15
     * 2020-01-01 10:16:00 | 6        | 10       | 12
     * 2020-01-01 10:17:00 | 6        | 9        | 0
     */
    COLUMN
}
