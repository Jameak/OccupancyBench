# Known limitations
The following is a list of known limitations of the benchmark. Some of these limitations are caused by differences in the functionality supported by some databases, while others were simply due to time-limitations during development.

## Benchmark limitations
* When using the wide schema format, ingestion is limited to using one thread. For the narrow schema ingestion can be split up among threads by delegating access points to each thread and having them perform their own batch-insertion. For the wide schema this isn't possible because each inserted row must contain data from all access points. This is potentially fixable by either creating a partial row-structure in the benchmark that all threads write to and dedicating a thread to batch-insert this data, or we could experiment with using upserts to insert partially finished rows.
* The benchmark can be run as multiple processes on various hosts against the same database. However, this setup is limited to one ingest-process and _N_ query-processes. The benchmark cannot run multiple ingest-processes concurrently against the same database because there is currently no way to delegate data-generation for specific access points to each process and no way to keep ingestion in sync across these processes.
* When partitioning is enabled for Apache Kudu, its limited query-language forces us to handle creation of partitions ourselves. To simplify our insertion implementation, we chose to pre-generate all the partitions. This may improve performance because the benchmark doesn't have to stop insertion to modify the partition-setup. However, because the full number of partitions is created immediately, this may also hurt performance when compared to other databases that create the partitions as needed depending on how many partitions are pre-generated.
* When configuring multiple query- and insertion-threads, each thread gets its own connection to the database. This design was chosen for simplicity, and because the thread-safety guarantees of the database libraries were unknown. Performance here could potentially be improved by sharing connections and for this purpose the benchmark has a setting to share the same database connection across threads. However, this requires both thread-safe database libraries (which we cant guarantee for all our databases) and a thread-safe query-/insertion-implementation (we know our current implementations aren't thread-safe) so this setting should not be enabled with the current implementations.

## Limitations caused by database issues
* The benchmark does not support intra-query parallelism for Kudu because of its minimal query-language. The benchmark could attempt to implement this using the functionality exposed by the Kudu library, but it would complicate thread control significantly when combined with the inter-query parallelism that the benchmark already performs. This limits the performance-benefit of running Kudu with more than 1 tablet-server in setups where data is partitioned such that entirely parallelized I/O and query-processing is possible because the benchmark cannot take advantage of this parallelism potential.
* The supported timestamp granularity differs between all our supported databases. InfluxDB supports nanoseconds, Kudu supports microseconds, and TimescaleDB supports milliseconds. While the benchmark can be configured to truncate timestamps to some common denominator to ensure that we use exactly the same data for all databases, this still means that the on-disk representation differs between all our databases which impacts file sizes and data-compression. There is no 'fix' for this beyond just being aware of this limitation during benchmarking.
* The usability of the wide schema is severely limited by the number of columns supported by the databases. This effectively limits the number of access points that this schema is usable for. TimescaleDB is limited to [~1600 columns](https://www.postgresql.org/docs/12/limits.html) for our use-case while Kudu has a soft cap of [300 columns](https://kudu.apache.org/docs/known_issues.html). InfluxDB seemingly has no hard limit.
* The InfluxDB library uses its own dedicated thread for batching insertions. While we have some control over how often it writes the batch to the database, we are forced to either perform single-row insertions on our own thread or batch-insertion on the libraries thread. This differs from our other benchmark implementations where we have full control over batch-sizes and can submit them manually on a thread that we control. The effect of this (combined with giving each insertion-thread its own database connection) is that InfluxDB uses 2x the number of configured threads for insertion (one thread to generate rows and add them to the batch, and one to write them to the database).
* Due to forced uniqueness constraints on inserted rows by supported databases (and a lack of support for auto-incrementing columns in some databases), we cannot represent multiple rows with identical timestamps and access point names. This would violate the uniqueness constraints that these systems rely on for their performance and architecture (Kudu will throw an error and InfluxDB will silently overwrite the old row. We're haven't checked the TimescaleDB behavior). This isn't necessarily a problem that the benchmark should attempt to fix because this use-case is extremely rare. However, we do not warn the user if their benchmark configuration is susceptible to this issue, which we could easily do.
* While implementing the wide schema we had to decide how to represent missing access point values since each row must contain all our columns. We could not use `null` across the board because InfluxDB does not support `null`-values so the remaining choice was between using an invalid value that the access point would never send (e.g. -1) or using 0. If we went with a invalid value, then SQL computations would need to filter out this value first which would hurt performance and we therefore ended up using 0. This means that when using the wide schema we cannot tell if an access point has crashed (and we therefore don't get values from it) or if there are simply no clients connected to it. We could potentially use `null` in databases that support this, but we elected to use the same value across all implementations for simplicity.
