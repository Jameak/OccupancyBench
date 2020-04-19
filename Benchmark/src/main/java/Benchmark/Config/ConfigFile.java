package Benchmark.Config;

import Benchmark.Databases.DBTargets;
import Benchmark.Databases.Kudu.KuduPartitionInterval;
import Benchmark.Databases.Kudu.KuduPartitionType;
import Benchmark.Databases.SchemaFormats;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;

/**
 * The config files accepted by this program is defined by this class.
 *
 * It uses the Java Properties class for loading existing config files, so config files must adhere to its format.
 * See https://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader) for full rules.
 *
 * The relevant highlights are:
 * - Literal backslashes '\' must be escaped (such as in Windows paths). Valid example: path = C:\\example\\path\\to\\file.example
 * - Whitespace between the property name and the property value are ignored. Whitespace after the property value is not ignored.
 * - Lines that start with '!' or '#' are comments.
 * - Single and double quotes " and ' do not need to be escaped.
 */
public class ConfigFile {
    /**
     * Type: Integer
     * The seed to use for the random number generator.
     *
     * Is not applicable if a previous Random-instance is deserialized
     * (that is, when serialization is enabled and the generator is disabled)
     */
    private static final String SEED = "benchmark.rngseed";
    private static final String SEED_DEFAULT = "" + new Random().nextInt(10000);
    private final int seed;

    /**
     * Type: A single accepted value. Accepted values are: ROW, COLUMN
     * The schema-format to use for data generation and querying.
     */
    private static final String SCHEMA = "benchmark.schema";
    private static final String SCHEMA_DEFAULT = SchemaFormats.ROW.toString();
    private final SchemaFormats schema;

    /**
     * Type: Boolean
     * If enabled, all statistics that have been enabled will be written to disk in CSV format in addition to being
     * written to standard out. CSV-files will be written just before the benchmark is finished, to avoid IO impacting
     * the performance. The separator is '\t'.
     */
    private static final String LOG_TO_CSV              = "benchmark.output.csv";
    private static final String LOG_TO_CSV_DEFAULT      = "false";
    /**
     * Type: String
     * The path to the folder where CSV files should be written.
     */
    private static final String LOG_TO_CSV_PATH         = "benchmark.output.csv.path";
    private static final String LOG_TO_CSV_PATH_DEFAULT = "FOLDER PATH";
    /**
     * Type: String
     * A prefix to include in the name of the folder that gets created at the csv.path target, to make it easier
     * to identify which folder belongs to which run of a program, when multiples have been scripted.
     *
     * Also, you can change this value if multiple iterations of the same config-file are being run,
     * to stop the csv-output from overwriting previous results. This will both make the folder-name change because the
     * config was changed, but also allow you to include the iteration-number in the name.
     *
     * If set to 'none', no prefix will be set.
     */
    private static final String CSV_FOLDER_PREFIX         = "benchmark.output.csv.prefix";
    private static final String CSV_FOLDER_PREFIX_DEFAULT = "NONE";
    /**
     * Type: Boolean
     * Controls whether the csv-files will include their header in the output file.
     */
    private static final String CSV_INCLUDE_HEADER         = "benchmark.output.csv.header";
    private static final String CSV_INCLUDE_HEADER_DEFAULT = "true";
    private final boolean logToCSV;
    private final String  logToCSVPath;
    private final String  csvFolderPrefix;
    private final boolean csvIncludeHeader;

    /**
     * Type: Boolean
     * Enables de-/serialization of generated floors and the state of the RNG after initial data generation.
     *
     * If both serialization and the generator is enabled, floor information and the RNG will be generated and then serialized.
     * If serialization is enabled and the generator is disabled, the program will attempt to deserialize this data instead.
     */
    private static final String SERIALIZE_ENABLED = "serialization.enabled";
    private static final String SERIALIZE_ENABLED_DEFAULT = "false";
    /**
     * Type: String
     * Path of the folder to de-/serialize data into/from.
     */
    private static final String SERIALIZE_PATH    = "serialization.path";
    private static final String SERIALIZE_PATH_DEFAULT = "FOLDER PATH";
    private final boolean   serialize;
    private final String    serializePath;

    /**
     * Type: Boolean
     * Controls whether floor information and initial data is generated.
     * Any generated initial data is added to the database.
     *
     * The generator is also used during ingestion, so set the generator-config settings accordingly even if
     * initial data generation is turned off.
     */
    private static final String GENERATOR_ENABLED                 = "generator.enabled";
    private static final String GENERATOR_ENABLED_DEFAULT         = "true";
    /**
     * Type: Double
     * The scaling applied to the generated data. A value of 1.0 resembles the amount of data generated in the ITU system.
     * Changing the scaling affects the number of floors and access-points that are generated, as well as the total number
     * of clients distributed among them.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_SCALE                   = "generator.data.scale";
    private static final String GENERATOR_SCALE_DEFAULT           = "1.0";
    /**
     * Type: A single accepted value. Accepted values are: NANOSECOND, MICROSECOND, MILLISECOND, SECOND, MINUTE
     * The granularity of the time-stamps inserted into the target database.
     * Data will be generated with nanosecond-granularity, truncated to this value, and then padded to the internally
     * supported granularity of the target database.
     *
     * Note that a granularity that is coarser than the specified {@code GENERATOR_GENERATION_INTERVAL} will result in writes
     * to the database that cannot be distinguished from each other because their timestamps will be truncated to the same value.
     *
     * Notes on granularity supported by database implementations:
     * - InfluxDB supports nanosecond-granularity (or coarser).
     * - TimescaleDB supports millisecond-granularity (or coarser).
     * - Apache Kudu supports microsecond-granularity (or coarser).
     *
     * If an unsupported granularity for the target database is used, generated values will automatically be truncated
     * to the expected granularity.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_GRANULARITY             = "generator.data.granularity";
    private static final String GENERATOR_GRANULARITY_DEFAULT     = Granularity.NANOSECOND.toString();
    /**
     * Type: Integer
     * A random number from 0 to this value will be multiplied with the probability of each AP and added to the
     * number of clients connected to it.
     *
     * A value of 0 results in data that exactly matches the seasonality and cache-behavior of the seed data (under the
     * assumption of a 1-1 mapping between seed-APs and generated APs which isn't quite true).
     * However, if a very small amount of seed data is available, then a value of 0 will generate data whose period is
     * the amount of data in the seed data. A smart database might be able to discover this period and vastly improve
     * cache-performance by exploiting it.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_JITTER                  = "generator.data.jitter";
    private static final String GENERATOR_JITTER_DEFAULT          = "5";
    /**
     * Type: String
     * The path to the mapping-file between seed AP-names and their assigned ID, as used in the probability-map
     * created by the python script.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_IDMAP                   = "generator.data.idmap";
    private static final String GENERATOR_IDMAP_DEFAULT           = "FILE PATH";
    /**
     * Type: String
     * The path to the folder containing the probability-map files as created by the python script.
     * This folder should ONLY contain probability-map files and nothing else.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_MAP_FOLDER              = "generator.data.folder";
    private static final String GENERATOR_MAP_FOLDER_DEFAULT      = "FOLDER PATH";
    /**
     * Type: Integer
     * The sampling rate in seconds for the AP-readings in the probability-map files.
     * Value must be evenly divisible by {@code GENERATOR_GENERATION_INTERVAL}.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_SEED_SAMPLE_RATE         = "generator.data.seedsamplerate";
    private static final String GENERATOR_SEED_SAMPLE_RATE_DEFAULT = "60";
    /**
     * Type: Integer
     * The sampling rate in seconds for the AP-readings to be generated by the generator.
     * Value must be evenly divisible by {@code GENERATOR_SOURCE_INTERVAL}.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_GENERATION_SAMPLE_RATE         = "generator.data.generationsamplerate";
    private static final String GENERATOR_GENERATION_SAMPLE_RATE_DEFAULT = "60";
    /**
     * Type: Boolean
     * Controls whether the mapping of data from seed APs to fake, generated APs should preserve floor-numbers.
     * That is, whether a seed AP from floor 3 must be assigned to a generated AP on the generated floor 3 or whether
     * it can be assigned to any floor.
     */
    private static final String GENERATOR_KEEP_FLOOR_ASSOCIATIONS = "generator.data.keepfloorassociations";
    private static final String GENERATOR_KEEP_FLOOR_ASSOCIATIONS_DEFAULT = "true";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The start date for initial data generation. Inclusive.
     */
    private static final String GENERATOR_START_DATE              = "generator.data.startdate";
    private static final String GENERATOR_START_DATE_DEFAULT      = "2019-01-01";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The end date for initial data generation. Exclusive.
     */
    private static final String GENERATOR_END_DATE                = "generator.data.enddate";
    private static final String GENERATOR_END_DATE_DEFAULT        = "2019-04-01";
    /**
     * Type: Comma-separated string of accepted values. Accepted values are: CSV, INFLUX, TIMESCALE, KUDU
     * The outputs to add generated data to. If multiple targets are specified, all targets receive data as it is generated.
     */
    private static final String GENERATOR_OUTPUT_TARGETS          = "generator.output.targets";
    private static final String GENERATOR_OUTPUT_TARGETS_DEFAULT  = DBTargets.INFLUX.toString();
    /**
     * Type: String
     * If CSV is specified in {@code GENERATOR_OUTPUT_TARGETS} then this is the path of the file that the generated
     * data is written to.
     */
    private static final String GENERATOR_OUTPUT_TO_DISK_TARGET   = "generator.output.filepath";
    private static final String GENERATOR_OUTPUT_TO_DISK_TARGET_DEFAULT = "TARGET FILE PATH";
    private final boolean   generatorEnabled;
    private final double    generatorScale;
    private final Granularity generatorGranularity;
    private final int       generatorJitter;
    private final String    generatorIdmap;
    private final String    generatorMapfolder;
    private final int       generatorSeedSamplerate;
    private final int       generatorGenerationSamplerate;
    private final boolean   generatorKeepFloorAssociations;
    private final LocalDate generatorStartDate;
    private final LocalDate generatorEndDate;
    private final DBTargets[] generatorOutputTargets;
    private final String    generatorToDiskTarget;

    /**
     * Type: String
     * The url of the Influx-database to connect to. Must include the port-number.
     */
    private static final String INFLUX_URL      = "influx.url";
    private static final String INFLUX_URL_DEFAULT = "localhost:8086";
    /**
     * Type: String
     * The username of the Influx user to use.
     */
    private static final String INFLUX_USERNAME = "influx.username";
    private static final String INFLUX_USERNAME_DEFAULT = "USERNAME";
    /**
     * Type: String
     * The password of the specified Influx user.
     */
    private static final String INFLUX_PASSWORD = "influx.password";
    private static final String INFLUX_PASSWORD_DEFAULT = "PASSWORD";
    /**
     * Type: String
     * The name of the database to generate/query.
     */
    private static final String INFLUX_DBNAME   = "influx.dbname";
    private static final String INFLUX_DBNAME_DEFAULT = "benchmark";
    /**
     * Type: String
     * The name of the Influx-measurement to generate/query.
     */
    private static final String INFLUX_TABLE     = "influx.table";
    private static final String INFLUX_TABLE_DEFAULT = "generated";
    /**
     * Type: Integer
     * The max number of inserts to batch together during generation/ingestion.
     * A batch-write is issued if either the batch-size or flush-time is reached
     */
    private static final String INFLUX_BATCHSIZE = "influx.batch.size";
    private static final String INFLUX_BATCHSIZE_DEFAULT = "10000";
    /**
     * Type: Integer
     * The max number of milliseconds between batch-writes.
     * A batch-write is issued if either the batch-size or flush-time is reached
     */
    private static final String INFLUX_BATCH_FLUSH_TIME = "influx.batch.flushtime";
    private static final String INFLUX_BATCH_FLUSH_TIME_DEFAULT = "1000";
    private final String  influxUrl;
    private final String  influxUsername;
    private final String  influxPassword;
    private final String  influxDBName;
    private final String  influxTable;
    private final int     influxBatchsize;
    private final int     influxFlushtime;

    /**
     * Type: String
     * The host of the Timescale-database to connect to. May include a port-number.
     */
    private static final String TIMESCALE_HOST      = "timescale.host";
    private static final String TIMESCALE_HOST_DEFAULT = "localhost:5432";
    /**
     * Type: String
     * The username of the Timescale user to use.
     */
    private static final String TIMESCALE_USERNAME = "timescale.username";
    private static final String TIMESCALE_USERNAME_DEFAULT = "USERNAME";
    /**
     * Type: String
     * The password of the specified Timescale user.
     */
    private static final String TIMESCALE_PASSWORD = "timescale.password";
    private static final String TIMESCALE_PASSWORD_DEFAULT = "PASSWORD";
    /**
     * Type: String
     * The name of the database to generate/query.
     * The database must have been created already.
     */
    private static final String TIMESCALE_DBNAME   = "timescale.dbname";
    private static final String TIMESCALE_DBNAME_DEFAULT = "benchmark";
    /**
     * Type: String
     * The name of the Timescale-table to generate/query.
     */
    private static final String TIMESCALE_TABLE    = "timescale.table";
    private static final String TIMESCALE_TABLE_DEFAULT = "generated";
    /**
     * Type: Integer
     * The number of inserts to batch together during generation/ingestion.
     */
    private static final String TIMESCALE_BATCHSIZE     = "timescale.batchsize";
    private static final String TIMESCALE_BATCHSIZE_DEFAULT = "10000";
    /**
     * Type: Boolean
     * Controls whether the property "reWriteBatchedInserts" is included in the Timescale connection string.
     */
    private static final String TIMESCALE_REWRITE_BATCH = "timescale.rewritebatchedinserts";
    private static final String TIMESCALE_REWRITE_BATCH_DEFAULT = "true";
    /**
     * Type: Boolean
     * If enabled, a secondary index on (AP, time) is created when using the ROW-schema.
     */
    private static final String TIMESCALE_CREATE_SECONDARY_INDEX = "timescale.createsecondaryindex";
    private static final String TIMESCALE_CREATE_SECONDARY_INDEX_DEFAULT = "false";
    private final String  timescaleHost;
    private final String  timescaleUsername;
    private final String  timescalePassword;
    private final String  timescaleDBName;
    private final String  timescaleTable;
    private final Integer timescaleBatchSize;
    private final boolean timescaleReWriteBatchedInserts;
    private final boolean timescaleCreateSecondaryIndex;

    /**
     * Type: String with single hostname or comma-separated list of masters
     * The host of the Kudu-database to connect to. Must include a port number.
     */
    private static final String KUDU_HOST     = "kudu.host";
    // These addresses correspond to the Kudu-masters from the Kudu quickstart docker setup.
    private static final String KUDU_HOST_DEFAULT = "localhost:7051,localhost:7151,localhost:7251";
    /**
     * Type: String
     * The name of the Kudu-table to generate/query.
     */
    private static final String KUDU_TABLE    = "kudu.table";
    private static final String KUDU_TABLE_DEFAULT = "generated";
    /**
     * Type: Integer
     * Max number of columns supported by Kudu.
     * By default Kudu limits the number of columns to 300. If more columns are desired,
     * provide Kudu with flags to increase the limit and then set the same value here.
     */
    private static final String KUDU_MAX_SUPPORTED_COLUMNS    = "kudu.maxcolumns";
    private static final String KUDU_MAX_SUPPORTED_COLUMNS_DEFAULT = "300";
    /**
     * Type: Integer
     * The number of inserts to batch together during generation/ingestion before a flush is issued.
     */
    private static final String KUDU_BATCH_SIZE = "kudu.batchsize";
    private static final String KUDU_BATCH_SIZE_DEFAULT = "1000";
    /**
     * Type: Integer
     * A value defining the size of the 'mutation buffer space' for the Kudu library.
     *
     * This does not have a 1-to-1 correlation with the batch size. If the batch size is too
     * big the buffer might overflow and throw an exception, in which case you'll need to
     * increase this value.
     */
    // NOTE: The internals of the Kudu client library sets this value to 1000 by default, but that's not guaranteed to be a good value.
    // NOTE: How big this ends up making the buffer (i.e. how many things can be added to the batch before the buffer is filled)
    //       is apparently not wired together with data-sizes, so e.g. a value of 1000 does not mean that a batch size of 1000 will fit
    //       (it might only fit 50, or might fit 5000... I'm unsure)
    private static final String KUDU_MUTATION_BUFFER_SPACE = "kudu.mutationbufferspace";
    private static final String KUDU_MUTATION_BUFFER_SPACE_DEFAULT = "1000";
    /**
     * Type: A single accepted value. Accepted values are: NONE, HASH, RANGE, HASH_AND_RANGE
     *
     * Controls how the benchmark-table is partitioned.
     * Read the Kudu-documentation for how hash- and range-partitioning interacts and its effect on the
     * number of partitions before using HASH_AND_RANGE.
     *
     * For the row-schema, all partitioning values are accepted.
     * For the column-schema, only RANGE is accepted since this schema doesn't have a column for which
     *   a suitable hash-column exists.
     */
    private static final String KUDU_PARTITION_TYPE = "kudu.partitioning.type";
    private static final String KUDU_PARTITION_TYPE_DEFAULT = KuduPartitionType.NONE.toString();
    /**
     * Type: Integer
     *
     * The number of buckets to hash the "AP" column into when using HASH-partitioning.
     */
    private static final String KUDU_HASH_PARTITION_BUCKETS = "kudu.partitioning.hash.buckets";
    private static final String KUDU_HASH_PARTITION_BUCKETS_DEFAULT = "4";
    /**
     * Type: A single accepted value. Accepted values are: WEEKLY, MONTHLY
     *
     * The interval of the range-partition created during table-creation.
     */
    private static final String KUDU_RANGE_PARTITION_INTERVAL = "kudu.partitioning.range.interval";
    private static final String KUDU_RANGE_PARTITION_INTERVAL_DEFAULT = KuduPartitionInterval.MONTHLY.toString();
    /**
     * Type: Integer
     *
     * The number of years of range-partitions to pre-create during table-creation.
     * The benchmark doesn't (currently) support creating new partitions on-the-fly during ingestion to keep
     * synchronization-overhead down, since Kudu can fill 1 year of data in less than 20 seconds in some configurations.
     *
     * Ingestion/generation will warn when the pre-created range-partitions have been exceeded. All writes from then on
     * land in a final pre-created unbounded partition.
     *
     * Be very careful about setting this value too high. A value of 10 should be considered the max, and in that case
     * a MONTHLY interval should be used to keep tablet-numbers reasonable.
     */
    private static final String KUDU_RANGE_PARTITION_PRECREATE_YEARS = "kudu.partitioning.range.precreatedyears";
    private static final String KUDU_RANGE_PARTITION_PRECREATE_YEARS_DEFAULT = "4";
    private final String kuduMasters;
    private final String kuduTable;
    private final int    kuduMaxColumns;
    private final int    kuduBatchSize;
    private final int    kuduMutationBufferSpace;
    private final int    kuduHashBuckets;
    private final int    kuduRangePrecreatedNumberOfYears;
    private final KuduPartitionType kuduPartitionType;
    private final KuduPartitionInterval kuduPartitionInterval;

    /**
     * Type: Boolean
     * Controls whether ingestion is enabled.
     */
    private static final String INGEST_ENABLED             = "ingest.enabled";
    private static final String INGEST_ENABLED_DEFAULT     = "true";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The first date to generate data for.
     */
    private static final String INGEST_START_DATE          = "ingest.startdate";
    private static final String INGEST_START_DATE_DEFAULT  = "2019-04-01";
    /**
     * Type: Integer
     * The desired number of entries for each ingestion-thread to generate per second.
     * The desired number of entries are spread out over each second with a variable delay to reach the desired speed.
     * A value <= 0 will not limit the ingestion-speed.
     */
    private static final String INGEST_SPEED               = "ingest.speed";
    private static final String INGEST_SPEED_DEFAULT       = "-1";
    /**
     * Type: Integer
     * The number of seconds between ingestion reporting the current ingest-speed.
     * A value of <= 0 will make ingestion not report any intermediate values.
     */
    private static final String INGEST_REPORT_FREQUENCY    = "ingest.reportfrequency";
    private static final String INGEST_REPORT_FREQUENCY_DEFAULT = "-1";
    /**
     * Type: Integer
     * The number of seconds to run ingestion for, if ingestion is run on its own.
     *
     * If queries are enabled, then {@code QUERIES_DURATION} controls how long to run ingestion for and this value is ignored.
     * If both this and {@code INGEST_DURATION_END_DATE} is specified then whichever duration is first reached will
     * terminate ingestion.
     */
    private static final String INGEST_STANDALONE_DURATION = "ingest.duration.time";
    private static final String INGEST_STANDALONE_DURATION_DEFAULT = "-1";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The date to end ingestion on, if ingestion is run on its own. This will ensure that ingestion runs from the
     * specified start-date up to this end-date.
     *
     * If queries are enabled, then {@code QUERIES_DURATION} controls how long to run ingestion for and this value is ignored.
     * If both this and {@code INGEST_STANDALONE_DURATION} is specified then whichever duration is first reached will
     * terminate ingestion.
     */
    private static final String INGEST_DURATION_END_DATE = "ingest.duration.enddate";
    private static final String INGEST_DURATION_END_DATE_DEFAULT = "9999-12-31";
    /**
     * Type: A single accepted value. Accepted values are: INFLUX, TIMESCALE, KUDU
     * The target to write generated ingest-data to.
     */
    private static final String INGEST_TARGET              = "ingest.target";
    private static final String INGEST_TARGET_DEFAULT      = DBTargets.INFLUX.toString();
    /**
     * Type: Boolean
     * Controls whether the configured target should be recreated (dropped, then created) before ingestion begins.
     */
    private static final String INGEST_TARGET_RECREATE     = "ingest.target.recreate";
    private static final String INGEST_TARGET_RECREATE_DEFAULT = "false";
    /**
     * Type: Boolean
     * Controls whether the ingest-threads share a single Target-instance or whether they each get one instance.
     *
     * A single instance allows sharing of handles and resources, but no external synchronization is provided.
     * The Target-implementation for the specified {@code INGEST_TARGET} must be thread-safe if this is enabled.
     */
    private static final String INGEST_SHARED_INSTANCE     = "ingest.target.sharedinstance";
    private static final String INGEST_SHARED_INSTANCE_DEFAULT = "false";
    /**
     * Type: Integer
     * The number of threads to create for ingestion. A dedicated ingest-threadpool is created with this many threads,
     * and this many ingest-tasks are then submitted to the pool.
     */
    private static final String INGEST_THREADS             = "ingest.threads";
    private static final String INGEST_THREADS_DEFAULT     = "1";
    private final boolean   ingestEnabled;
    private final LocalDate ingestStartDate;
    private final int       ingestSpeed;
    private final int       ingestReportFrequency;
    private final int       ingestDurationStandalone;
    private final LocalDate ingestDurationEndDate;
    private final DBTargets ingestTarget;
    private final boolean   ingestTargetRecreate;
    private final boolean   ingestTargetSharedInstance;
    private final int       ingestThreads;

    /**
     * Type: Boolean
     * Controls whether queries are enabled.
     */
    private static final String QUERIES_ENABLED                  = "queries.enabled";
    private static final String QUERIES_ENABLED_DEFAULT          = "true";
    /**
     * Type: A single accepted value. Accepted values are: INFLUX, TIMESCALE, KUDU
     * The target to run queries against.
     */
    private static final String QUERIES_TARGET                   = "queries.target";
    private static final String QUERIES_TARGET_DEFAULT           = DBTargets.INFLUX.toString();
    /**
     * Type: Boolean
     * Controls whether the query-threads share a single Queries-instance or whether they each get one instance.
     *
     * A single instance allows sharing of handles and resources, but no external synchronization is provided.
     * The Queries-implementation for the specified {@code QUERIES_TARGET} must be thread-safe if this is enabled.
     */
    private static final String QUERIES_SHARED_INSTANCE          = "queries.target.sharedinstance";
    private static final String QUERIES_SHARED_INSTANCE_DEFAULT  = "false";
    /**
     * Type: Integer
     * The number of threads to create for querying. A dedicated query-threadpool is created with this many threads,
     * and this many query-tasks are then submitted to the pool.
     */
    private static final String QUERIES_THREADS                  = "queries.threads";
    private static final String QUERIES_THREADS_DEFAULT          = "1";
    /**
     * Type: Integer
     * The number of seconds to run the queries for before stopping.
     */
    private static final String QUERIES_DURATION                 = "queries.duration.time";
    private static final String QUERIES_DURATION_DEFAULT         = "60";
    /**
     * Type: Integer
     * The number of seconds to do query-warmup for. Warmup runs the exact same code as normal queries, but doesn't
     * collect statistics.
     */
    private static final String QUERIES_WARMUP                   = "queries.duration.warmup";
    private static final String QUERIES_WARMUP_DEFAULT           = "5";
    /**
     * Type: Integer
     * The number of queries to run before stopping. Only considered if {@code QUERIES_DURATION} is <= 0.
     */
    private static final String QUERIES_MAX_COUNT                = "queries.duration.count";
    private static final String QUERIES_MAX_COUNT_DEFAULT        = "-1";
    /**
     * Type: Integer
     * The number of seconds between queries reporting query-statistics during query-execution.
     * In-progress query statistics will report the average query-speeds since the previous report, whereas the
     * final report prints values over the entire query-duration.
     *
     * A value of <= 0 will make querying not report any intermediate values.
     */
    private static final String QUERIES_REPORT_FREQUENCY         = "queries.reporting.summaryfrequency";
    private static final String QUERIES_REPORT_FREQUENCY_DEFAULT = "20";

    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The earliest possible date for queries to ask for.
     * Set this value to the first date in the database if the entire data-set is under consideration,
     * or set it to a later date if values earlier than this date should never be queried.
     */
    public static final String QUERIES_EARLIEST_VALID_DATE       = "queries.earliestdate";
    public static final String QUERIES_EARLIEST_VALID_DATE_DEFAULT = "2019-01-01";
    /**
     * Type: Integer
     * The weight of the 'Total Clients' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     *
     * Example: If 3 queries have the weights 1,1,2 then queries #1 and #2 have a 25% chance of being selected,
     *          while query #3 has a 50% chance of being selected.
     */
    private static final String QUERIES_WEIGHT_TOTAL_CLIENTS     = "queries.weight.totalclients";
    private static final String QUERIES_WEIGHT_TOTAL_CLIENTS_DEFAULT = "1";
    /**
     * Type: Integer
     * The weight of the 'Floor Totals' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_FLOOR_TOTALS      = "queries.weight.floortotals";
    private static final String QUERIES_WEIGHT_FLOOR_TOTALS_DEFAUlT = "1";
    /**
     * Type: Integer
     * The weight of the 'Max for AP' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_MAX_FOR_AP      = "queries.weight.maxforap";
    private static final String QUERIES_WEIGHT_MAX_FOR_AP_DEFAULT = "2";
    /**
     * Type: Integer
     * The weight of the 'Avg occupancy' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_AVG_OCCUPANCY      = "queries.weight.avgoccupancy";
    private static final String QUERIES_WEIGHT_AVG_OCCUPANCY_DEFAULT = "1";
    /**
     * Type: Integer
     * The weight of the 'K-Means' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_KMEANS            = "queries.weight.kmeans";
    private static final String QUERIES_WEIGHT_KMEANS_DEFAULT    = "1";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value, then the time-interval
     * used in the query will at-most range over 24 hours, and will be at-most 24 hours old.
     *
     * That is, if P(X < {@code QUERIES_RNG_RANGE_DAY}) then this setting is used for the time-component of queries.
     */
    private static final String QUERIES_RNG_RANGE_DAY            = "queries.range.day";
    private static final String QUERIES_RNG_RANGE_DAY_DEFAULT    = "0.4";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value and greater than {@code QUERIES_RNG_RANGE_DAY},
     * then the time-interval used in the query will at-most range over 7 days and will be at-most 7 days old.
     *
     * That is, if P({@code QUERIES_RNG_RANGE_DAY} <= X < {@code QUERIES_RNG_RANGE_WEEK}) then this setting is used
     * for the time-component of queries.
     */
    private static final String QUERIES_RNG_RANGE_WEEK           = "queries.range.week";
    private static final String QUERIES_RNG_RANGE_WEEK_DEFAULT   = "0.7";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value and greater than {@code QUERIES_RNG_RANGE_WEEK},
     * then the time-interval used in the query will at-most range over 30 days, and will be at-most 30 days old.
     *
     * That is, if P({@code QUERIES_RNG_RANGE_WEEK} <= X < {@code QUERIES_RNG_RANGE_MONTH}) then this setting is used
     * for the time-component of queries.
     */
    private static final String QUERIES_RNG_RANGE_MONTH          = "queries.range.month";
    private static final String QUERIES_RNG_RANGE_MONTH_DEFAULT  = "0.9";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value and greater than {@code QUERIES_RNG_RANGE_MONTH},
     * then the time-interval used in the query will at-most range over 365 days, and will be at-most 365 days old.
     *
     * That is, if P({@code QUERIES_RNG_RANGE_MONTH} <= X < {@code QUERIES_RNG_RANGE_YEAR}) then this setting is used
     * for the time-component of queries.
     * Values exceeding this value (that is, values where P({@code QUERIES_RNG_RANGE_YEAR} < X)) may be selected from
     * the entire interval ranging from the newest inserted value to {@code QUERIES_EARLIEST_VALID_DATE}.
     */
    private static final String QUERIES_RNG_RANGE_YEAR           = "queries.range.year";
    private static final String QUERIES_RNG_RANGE_YEAR_DEFAULT   = "0.95";
    /**
     * Type: Integer
     * The minimal length of a time-span to query for, in seconds. Spans shorter than this duration will be extended,
     * biased towards new values.
     */
    private static final String QUERIES_INTERVAL_MIN             = "queries.interval.min";
    private static final String QUERIES_INTERVAL_MIN_DEFAULT     = "21600"; // 6 hours in seconds
    /**
     * Type: Integer
     * The maximum length of a time-span to query for, in seconds. Spans longer than this duration will be shortened,
     * biased towards old values.
     */
    private static final String QUERIES_INTERVAL_MAX             = "queries.interval.max";
    private static final String QUERIES_INTERVAL_MAX_DEFAULT     = "7776000"; // 90 days in seconds
    /**
     * Type: Integer
     * The minimal length of a time-span to query for when running the K-Means query, in seconds.
     * Spans shorter than this duration will be extended, biased towards new values.
     */
    private static final String QUERIES_INTERVAL_MIN_KMEANS      = "queries.interval.min.kmeans";
    private static final String QUERIES_INTERVAL_MIN_KMEANS_DEFAULT = "21600"; // 6 hours in seconds
    /**
     * Type: Integer
     * The maximum length of a time-span to query for when running the K-Means query, in seconds.
     * Spans longer than this duration will be shortened, biased towards old values.
     */
    private static final String QUERIES_INTERVAL_MAX_KMEANS      = "queries.interval.max.kmeans";
    private static final String QUERIES_INTERVAL_MAX_KMEANS_DEFAULT = "86400"; // 24 hours in seconds
    /**
     * Type: Integer
     * The number of clusters to use for the K-Means computation.
     */
    private static final String QUERIES_KMEANS_CLUSTERS          = "queries.kmeans.clusters";
    private static final String QUERIES_KMEANS_CLUSTERS_DEFAULT  = "5";
    /**
     * Type: Integer
     * The number of iterations to run during the K-Means computation.
     */
    private static final String QUERIES_KMEANS_ITERATIONS        = "queries.kmeans.iterations";
    private static final String QUERIES_KMEANS_ITERATIONS_DEFAULT= "10";
    /**
     * Type: Integer
     * Specifies how often (in milliseconds) the query-executor queries the database for the newest inserted value.
     * The returned value will be used as the newest possible value that can be used in a query.
     *
     * A value of 0 will make the query-executor retrieve the newest value before each query is run.
     *
     * A value less than 0 will make ingestion-threads tell the query-executor what the newest value is directly without
     * having to ask the database. However, this has the issue of the newest value that was inserted not necessarily
     * being the newest possible queryable value (such as if the database employs snapshotting, or because the value has
     * been added to the batch but not written yet), potentially making queries on the newest values too easy because no
     * data is returned.
     */
    private static final String QUERIES_DATE_COMM                = "queries.dateinformation";
    private static final String QUERIES_DATE_COMM_DEFAULT        = "500";
    private final boolean   queriesEnabled;
    private final DBTargets queriesTarget;
    private final int       queriesThreads;
    private final boolean   queriesSharedInstance;
    private final int       queriesDuration;
    private final int       queriesWarmup;
    private final int       queriesMaxCount;
    private final int       queriesReportFrequency;
    private final LocalDate queriesEarliestValidDate;
    private final int       queriesWeightTotalClients;
    private final int       queriesWeightFloorTotals;
    private final int       queriesWeightMaxForAP;
    private final int       queriesWeightAvgOccupancy;
    private final int       queriesWeightKMeans;
    private final double    queriesRngRangeDay;
    private final double    queriesRngRangeWeek;
    private final double    queriesRngRangeMonth;
    private final double    queriesRngRangeYear;
    private final int       queriesIntervalMin;
    private final int       queriesIntervalMax;
    private final int       queriesIntervalMinKMeans;
    private final int       queriesIntervalMaxKMeans;
    private final int       queriesKMeansClusters;
    private final int       queriesKMeansIterations;
    private final int       queriesDateCommIntervalMilliseconds;

    /**
     * Type: Boolean
     * Controls whether to generate tables containing pre-computed data for the purposes of debugging and graphing.
     * Only implemented for influx since this is purely for debugging.
     */
    private static final String DEBUG_CREATE_PRECOMPUTED_TABLES = "debug.createprecomputedtables";
    private static final String DEBUG_CREATE_PRECOMPUTED_TABLES_DEFAULT = "false";
    /**
     * Type: Boolean
     * If enabled, all settings are printed at program start. Both those specified in the loaded config and the default
     * values are printed.
     */
    private static final String DEBUG_PRINT_ALL_SETTINGS = "debug.printallsettings";
    private static final String DEBUG_PRINT_ALL_SETTINGS_DEFAULT = "false";
    /**
     * Type: Boolean
     * If enabled, all values returned by the executed queries will be written to disk to facilitate easy inspection.
     *
     * Files will be written to {@code DEBUG_SAVE_QUERY_RESULTS_PATH}.
     */
    private static final String DEBUG_SAVE_QUERY_RESULTS = "debug.savequeryresults";
    private static final String DEBUG_SAVE_QUERY_RESULTS_DEFAULT = "false";
    /**
     * Type: String
     * The path to write the query results to.
     */
    private static final String DEBUG_SAVE_QUERY_RESULTS_PATH = "debug.savequeryresults.path";
    private static final String DEBUG_SAVE_QUERY_RESULTS_PATH_DEFAULT = "TARGET FOLDER PATH";
    /**
     * Type: Boolean
     * Governs whether to synchronize the rng state for data generation across both ROW and COLUMN schemes.
     * The effect of this ensures that the exact same data is generated for both, making them directly comparable at
     * the cost of slightly more generation overhead.
     */
    private static final String DEBUG_SYNCHRONIZE_RNG_STATE = "debug.synchronizerngstate";
    private static final String DEBUG_SYNCHRONIZE_RNG_STATE_DEFAULT = "false";
    /**
     * Type: Boolean
     * Governs whether to truncate the timestamps used in queries to minutes.
     *
     * This is useful for comparing query-results between row- and column-schemas. Specifically, this ensures that
     * if we have the following entries in the row- and column-databases:
     * ROW DB:    TIME (HH:MM:SS) | AP   | CLIENTS
     *            10:00:01        | AP-1 | 5
     *            10:00:02        | AP-2 | 10
     *            10:00:03        | AP-3 | 15
     * COLUMN DB: TIME (HH:MM:SS) | AP-1 | AP-2 | AP-3
     *            10:00:03        | 5    | 10   | 15
     * then a randomly generated query-timerange of "start: 10:00:02" and "end: 10:10:00" will be truncated to querying
     * "start: 10:00:00" which ensures that the query grabs the entirety of the row-entries instead of only some of them.
     *
     * For row- vs column-schema queries to be comparable we need to always grab either all entries or none, which
     * enabling this setting ensures that we do. If we dont care about them being comparable (such as during normal
     * benchmark operation) then comparability doesn't really matter and this can be turned off.
     */
    private static final String DEBUG_TRUNCATE_QUERY_TIMESTAMPS = "debug.truncatequerytimestamps";
    private static final String DEBUG_TRUNCATE_QUERY_TIMESTAMPS_DEFAULT = "false";
    /**
     * Type: Boolean
     * Governs whether to report debug information from the query-runner about each individual query as its running.
     * This prints info such as which query is running, it's arguments, and how long it took to complete.
     *
     * This is useful for debugging during execution but if you're just interested in how long each query took to
     * complete then use the csv-config for an output that's easier to parse.
     */
    private static final String DEBUG_REPORT_QUERY_STATUS         = "debug.reportquerystatus";
    private static final String DEBUG_REPORT_QUERY_STATUS_DEFAULT = "false";
    private final boolean debugCreatePrecomputedTables;
    private final boolean debugPrintSettings;
    private final boolean debugSaveQueryResults;
    private final String  debugSaveQueryResultsPath;
    private final boolean debugSynchronizeRngState;
    private final boolean debugTruncateQueryTimestamps;
    private final boolean debugReportQueryStatus;

    private final Properties prop;
    private boolean validated;
    private String validationError = "NO ERROR";

    public static ConfigFile load(String... filePaths) throws IOException {
        assert filePaths.length != 0 : "Why are you loading 0 config files?";

        Set<String> encounteredPropNames = new HashSet<>();
        Properties combinedProp = new Properties(defaultProp());
        for(String filePath : filePaths){
            Properties prop = new Properties();
            try(InputStream input = new FileInputStream(filePath)){
                prop.load(input);
            }

            for(String name : prop.stringPropertyNames()){
                if(encounteredPropNames.contains(name)){
                    System.out.println("Warning: Encountered duplicate property with name '" + name + "' in file:" + filePath);
                    System.out.println("    Old value: " + combinedProp.getProperty(name));
                    System.out.println("    New value: " + prop.getProperty(name));
                    combinedProp.setProperty(name, prop.getProperty(name));
                } else {
                    combinedProp.setProperty(name, prop.getProperty(name));
                    encounteredPropNames.add(name);
                }
            }
        }

        ConfigFile config = new ConfigFile(combinedProp);

        String error = config.validateConfig();
        assert error == null : error;
        if(error == null) config.validated = true;
        else config.validationError = error;

        return config;
    }

    public static ConfigFile defaultConfig(){
        return new ConfigFile(defaultProp());
    }

    private static Properties defaultProp(){
        Properties prop = new Properties();

        //Benchmark
        prop.setProperty(SEED, SEED_DEFAULT);
        prop.setProperty(SCHEMA, SCHEMA_DEFAULT);
        prop.setProperty(LOG_TO_CSV, LOG_TO_CSV_DEFAULT);
        prop.setProperty(LOG_TO_CSV_PATH, LOG_TO_CSV_PATH_DEFAULT);
        prop.setProperty(CSV_FOLDER_PREFIX, CSV_FOLDER_PREFIX_DEFAULT);
        prop.setProperty(CSV_INCLUDE_HEADER, CSV_INCLUDE_HEADER_DEFAULT);

        //Serialization
        prop.setProperty(SERIALIZE_ENABLED, SERIALIZE_ENABLED_DEFAULT);
        prop.setProperty(SERIALIZE_PATH, SERIALIZE_PATH_DEFAULT);

        //Generator
        prop.setProperty(GENERATOR_ENABLED, GENERATOR_ENABLED_DEFAULT);
        prop.setProperty(GENERATOR_SCALE, GENERATOR_SCALE_DEFAULT);
        prop.setProperty(GENERATOR_GRANULARITY, GENERATOR_GRANULARITY_DEFAULT);
        prop.setProperty(GENERATOR_JITTER, GENERATOR_JITTER_DEFAULT);
        prop.setProperty(GENERATOR_IDMAP, GENERATOR_IDMAP_DEFAULT);
        prop.setProperty(GENERATOR_MAP_FOLDER, GENERATOR_MAP_FOLDER_DEFAULT);
        prop.setProperty(GENERATOR_SEED_SAMPLE_RATE, GENERATOR_SEED_SAMPLE_RATE_DEFAULT);
        prop.setProperty(GENERATOR_GENERATION_SAMPLE_RATE, GENERATOR_GENERATION_SAMPLE_RATE_DEFAULT);
        prop.setProperty(GENERATOR_KEEP_FLOOR_ASSOCIATIONS, GENERATOR_KEEP_FLOOR_ASSOCIATIONS_DEFAULT);
        prop.setProperty(GENERATOR_START_DATE, GENERATOR_START_DATE_DEFAULT);
        prop.setProperty(GENERATOR_END_DATE, GENERATOR_END_DATE_DEFAULT);
        prop.setProperty(GENERATOR_OUTPUT_TARGETS, GENERATOR_OUTPUT_TARGETS_DEFAULT);
        prop.setProperty(GENERATOR_OUTPUT_TO_DISK_TARGET, GENERATOR_OUTPUT_TO_DISK_TARGET_DEFAULT);

        //Influx
        prop.setProperty(INFLUX_URL, INFLUX_URL_DEFAULT);
        prop.setProperty(INFLUX_USERNAME, INFLUX_USERNAME_DEFAULT);
        prop.setProperty(INFLUX_PASSWORD, INFLUX_PASSWORD_DEFAULT);
        prop.setProperty(INFLUX_DBNAME, INFLUX_DBNAME_DEFAULT);
        prop.setProperty(INFLUX_TABLE, INFLUX_TABLE_DEFAULT);
        prop.setProperty(INFLUX_BATCHSIZE, INFLUX_BATCHSIZE_DEFAULT);
        prop.setProperty(INFLUX_BATCH_FLUSH_TIME, INFLUX_BATCH_FLUSH_TIME_DEFAULT);

        //Timescale
        prop.setProperty(TIMESCALE_HOST, TIMESCALE_HOST_DEFAULT);
        prop.setProperty(TIMESCALE_USERNAME, TIMESCALE_USERNAME_DEFAULT);
        prop.setProperty(TIMESCALE_PASSWORD, TIMESCALE_PASSWORD_DEFAULT);
        prop.setProperty(TIMESCALE_DBNAME, TIMESCALE_DBNAME_DEFAULT);
        prop.setProperty(TIMESCALE_TABLE, TIMESCALE_TABLE_DEFAULT);
        prop.setProperty(TIMESCALE_BATCHSIZE, TIMESCALE_BATCHSIZE_DEFAULT);
        prop.setProperty(TIMESCALE_REWRITE_BATCH, TIMESCALE_REWRITE_BATCH_DEFAULT);
        prop.setProperty(TIMESCALE_CREATE_SECONDARY_INDEX, TIMESCALE_CREATE_SECONDARY_INDEX_DEFAULT);

        //Kudu
        prop.setProperty(KUDU_HOST, KUDU_HOST_DEFAULT);
        prop.setProperty(KUDU_TABLE, KUDU_TABLE_DEFAULT);
        prop.setProperty(KUDU_MAX_SUPPORTED_COLUMNS, KUDU_MAX_SUPPORTED_COLUMNS_DEFAULT);
        prop.setProperty(KUDU_BATCH_SIZE, KUDU_BATCH_SIZE_DEFAULT);
        prop.setProperty(KUDU_MUTATION_BUFFER_SPACE, KUDU_MUTATION_BUFFER_SPACE_DEFAULT);
        prop.setProperty(KUDU_PARTITION_TYPE, KUDU_PARTITION_TYPE_DEFAULT);
        prop.setProperty(KUDU_HASH_PARTITION_BUCKETS, KUDU_HASH_PARTITION_BUCKETS_DEFAULT);
        prop.setProperty(KUDU_RANGE_PARTITION_INTERVAL, KUDU_RANGE_PARTITION_INTERVAL_DEFAULT);
        prop.setProperty(KUDU_RANGE_PARTITION_PRECREATE_YEARS, KUDU_RANGE_PARTITION_PRECREATE_YEARS_DEFAULT);

        //Ingest
        prop.setProperty(INGEST_ENABLED, INGEST_ENABLED_DEFAULT);
        prop.setProperty(INGEST_START_DATE, INGEST_START_DATE_DEFAULT);
        prop.setProperty(INGEST_SPEED, INGEST_SPEED_DEFAULT);
        prop.setProperty(INGEST_REPORT_FREQUENCY, INGEST_REPORT_FREQUENCY_DEFAULT);
        prop.setProperty(INGEST_STANDALONE_DURATION, INGEST_STANDALONE_DURATION_DEFAULT);
        prop.setProperty(INGEST_DURATION_END_DATE, INGEST_DURATION_END_DATE_DEFAULT);
        prop.setProperty(INGEST_TARGET, INGEST_TARGET_DEFAULT);
        prop.setProperty(INGEST_TARGET_RECREATE, INGEST_TARGET_RECREATE_DEFAULT);
        prop.setProperty(INGEST_SHARED_INSTANCE, INGEST_SHARED_INSTANCE_DEFAULT);
        prop.setProperty(INGEST_THREADS, INGEST_THREADS_DEFAULT);

        //Queries
        prop.setProperty(QUERIES_ENABLED, QUERIES_ENABLED_DEFAULT);
        prop.setProperty(QUERIES_TARGET, QUERIES_TARGET_DEFAULT);
        prop.setProperty(QUERIES_THREADS, QUERIES_THREADS_DEFAULT);
        prop.setProperty(QUERIES_SHARED_INSTANCE, QUERIES_SHARED_INSTANCE_DEFAULT);
        prop.setProperty(QUERIES_DURATION, QUERIES_DURATION_DEFAULT);
        prop.setProperty(QUERIES_WARMUP, QUERIES_WARMUP_DEFAULT);
        prop.setProperty(QUERIES_MAX_COUNT, QUERIES_MAX_COUNT_DEFAULT);
        prop.setProperty(QUERIES_REPORT_FREQUENCY, QUERIES_REPORT_FREQUENCY_DEFAULT);
        prop.setProperty(QUERIES_EARLIEST_VALID_DATE, QUERIES_EARLIEST_VALID_DATE_DEFAULT);
        prop.setProperty(QUERIES_WEIGHT_TOTAL_CLIENTS, QUERIES_WEIGHT_TOTAL_CLIENTS_DEFAULT);
        prop.setProperty(QUERIES_WEIGHT_FLOOR_TOTALS, QUERIES_WEIGHT_FLOOR_TOTALS_DEFAUlT);
        prop.setProperty(QUERIES_WEIGHT_MAX_FOR_AP, QUERIES_WEIGHT_MAX_FOR_AP_DEFAULT);
        prop.setProperty(QUERIES_WEIGHT_AVG_OCCUPANCY, QUERIES_WEIGHT_AVG_OCCUPANCY_DEFAULT);
        prop.setProperty(QUERIES_WEIGHT_KMEANS, QUERIES_WEIGHT_KMEANS_DEFAULT);
        prop.setProperty(QUERIES_RNG_RANGE_DAY  , QUERIES_RNG_RANGE_DAY_DEFAULT);
        prop.setProperty(QUERIES_RNG_RANGE_WEEK , QUERIES_RNG_RANGE_WEEK_DEFAULT);
        prop.setProperty(QUERIES_RNG_RANGE_MONTH, QUERIES_RNG_RANGE_MONTH_DEFAULT);
        prop.setProperty(QUERIES_RNG_RANGE_YEAR , QUERIES_RNG_RANGE_YEAR_DEFAULT);
        prop.setProperty(QUERIES_INTERVAL_MIN   , QUERIES_INTERVAL_MIN_DEFAULT);
        prop.setProperty(QUERIES_INTERVAL_MAX   , QUERIES_INTERVAL_MAX_DEFAULT);
        prop.setProperty(QUERIES_INTERVAL_MIN_KMEANS, QUERIES_INTERVAL_MIN_KMEANS_DEFAULT);
        prop.setProperty(QUERIES_INTERVAL_MAX_KMEANS, QUERIES_INTERVAL_MAX_KMEANS_DEFAULT);
        prop.setProperty(QUERIES_KMEANS_CLUSTERS, QUERIES_KMEANS_CLUSTERS_DEFAULT);
        prop.setProperty(QUERIES_KMEANS_ITERATIONS, QUERIES_KMEANS_ITERATIONS_DEFAULT);
        prop.setProperty(QUERIES_DATE_COMM      , QUERIES_DATE_COMM_DEFAULT);

        //Debug
        prop.setProperty(DEBUG_CREATE_PRECOMPUTED_TABLES, DEBUG_CREATE_PRECOMPUTED_TABLES_DEFAULT);
        prop.setProperty(DEBUG_PRINT_ALL_SETTINGS, DEBUG_PRINT_ALL_SETTINGS_DEFAULT);
        prop.setProperty(DEBUG_SAVE_QUERY_RESULTS, DEBUG_SAVE_QUERY_RESULTS_DEFAULT);
        prop.setProperty(DEBUG_SAVE_QUERY_RESULTS_PATH, DEBUG_SAVE_QUERY_RESULTS_PATH_DEFAULT);
        prop.setProperty(DEBUG_SYNCHRONIZE_RNG_STATE, DEBUG_SYNCHRONIZE_RNG_STATE_DEFAULT);
        prop.setProperty(DEBUG_TRUNCATE_QUERY_TIMESTAMPS, DEBUG_TRUNCATE_QUERY_TIMESTAMPS_DEFAULT);
        prop.setProperty(DEBUG_REPORT_QUERY_STATUS, DEBUG_REPORT_QUERY_STATUS_DEFAULT);

        return prop;
    }

    private ConfigFile(Properties properties){
        this.prop = properties;

        //Benchmark
        seed             = Integer.parseInt(     prop.getProperty(SEED).trim());
        schema           = SchemaFormats.valueOf(prop.getProperty(SCHEMA).toUpperCase().trim());
        logToCSV         = Boolean.parseBoolean( prop.getProperty(LOG_TO_CSV).trim());
        logToCSVPath     =                       prop.getProperty(LOG_TO_CSV_PATH);
        csvFolderPrefix  =                       prop.getProperty(CSV_FOLDER_PREFIX);
        csvIncludeHeader = Boolean.parseBoolean( prop.getProperty(CSV_INCLUDE_HEADER).trim());

        //Serialization
        serialize        = Boolean.parseBoolean(prop.getProperty(SERIALIZE_ENABLED).trim());
        serializePath    =                      prop.getProperty(SERIALIZE_PATH);

        //Generator
        generatorEnabled               = Boolean.parseBoolean(prop.getProperty(GENERATOR_ENABLED).trim());
        generatorIdmap                 =                      prop.getProperty(GENERATOR_IDMAP);
        generatorGranularity           = Granularity.valueOf( prop.getProperty(GENERATOR_GRANULARITY).toUpperCase().trim());
        generatorJitter                = Integer.parseInt(    prop.getProperty(GENERATOR_JITTER).trim());
        generatorScale                 = Double.parseDouble(  prop.getProperty(GENERATOR_SCALE).trim());
        generatorMapfolder             =                      prop.getProperty(GENERATOR_MAP_FOLDER);
        generatorSeedSamplerate        = Integer.parseInt(    prop.getProperty(GENERATOR_SEED_SAMPLE_RATE).trim());
        generatorGenerationSamplerate  = Integer.parseInt(    prop.getProperty(GENERATOR_GENERATION_SAMPLE_RATE).trim());
        generatorKeepFloorAssociations = Boolean.parseBoolean(prop.getProperty(GENERATOR_KEEP_FLOOR_ASSOCIATIONS).trim());
        generatorStartDate             = LocalDate.parse(     prop.getProperty(GENERATOR_START_DATE).trim());
        generatorEndDate               = LocalDate.parse(     prop.getProperty(GENERATOR_END_DATE).trim());
        generatorOutputTargets         = Arrays.stream(       prop.getProperty(GENERATOR_OUTPUT_TARGETS).split(","))
                .map(String::toUpperCase).map(String::trim).map(DBTargets::valueOf).toArray(DBTargets[]::new);
        generatorToDiskTarget          =                      prop.getProperty(GENERATOR_OUTPUT_TO_DISK_TARGET);

        //Influx
        String influxUrlInput =            prop.getProperty(INFLUX_URL);
        influxUrl       =                  influxUrlInput.startsWith("http://") || influxUrlInput.startsWith("https://") ? influxUrlInput : "http://" + influxUrlInput;
        influxUsername  =                  prop.getProperty(INFLUX_USERNAME);
        influxPassword  =                  prop.getProperty(INFLUX_PASSWORD);
        influxDBName    =                  prop.getProperty(INFLUX_DBNAME);
        influxTable     =                  prop.getProperty(INFLUX_TABLE);
        influxBatchsize = Integer.parseInt(prop.getProperty(INFLUX_BATCHSIZE).trim());
        influxFlushtime = Integer.parseInt(prop.getProperty(INFLUX_BATCH_FLUSH_TIME).trim());

        //Timescale
        timescaleHost                  =                      prop.getProperty(TIMESCALE_HOST);
        timescaleUsername              =                      prop.getProperty(TIMESCALE_USERNAME);
        timescalePassword              =                      prop.getProperty(TIMESCALE_PASSWORD);
        timescaleDBName                =                      prop.getProperty(TIMESCALE_DBNAME);
        timescaleTable                 =                      prop.getProperty(TIMESCALE_TABLE);
        timescaleBatchSize             = Integer.parseInt(    prop.getProperty(TIMESCALE_BATCHSIZE).trim());
        timescaleReWriteBatchedInserts = Boolean.parseBoolean(prop.getProperty(TIMESCALE_REWRITE_BATCH).trim());
        timescaleCreateSecondaryIndex  = Boolean.parseBoolean(prop.getProperty(TIMESCALE_CREATE_SECONDARY_INDEX).trim());

        //Kudu
        kuduMasters             =                  prop.getProperty(KUDU_HOST);
        kuduTable               =                  prop.getProperty(KUDU_TABLE);
        kuduMaxColumns          = Integer.parseInt(prop.getProperty(KUDU_MAX_SUPPORTED_COLUMNS).trim());
        kuduBatchSize           = Integer.parseInt(prop.getProperty(KUDU_BATCH_SIZE).trim());
        kuduMutationBufferSpace = Integer.parseInt(prop.getProperty(KUDU_MUTATION_BUFFER_SPACE).trim());
        kuduHashBuckets         = Integer.parseInt(prop.getProperty(KUDU_HASH_PARTITION_BUCKETS).trim());
        kuduRangePrecreatedNumberOfYears = Integer.parseInt(prop.getProperty(KUDU_RANGE_PARTITION_PRECREATE_YEARS).trim());
        kuduPartitionInterval   = KuduPartitionInterval.valueOf(prop.getProperty(KUDU_RANGE_PARTITION_INTERVAL).toUpperCase().trim());
        kuduPartitionType       = KuduPartitionType.valueOf(prop.getProperty(KUDU_PARTITION_TYPE).toUpperCase().trim());

        //Ingest
        ingestEnabled              = Boolean.parseBoolean(prop.getProperty(INGEST_ENABLED).trim());
        ingestStartDate            = LocalDate.parse(     prop.getProperty(INGEST_START_DATE).trim());
        ingestSpeed                = Integer.parseInt(    prop.getProperty(INGEST_SPEED).trim());
        ingestReportFrequency      = Integer.parseInt(    prop.getProperty(INGEST_REPORT_FREQUENCY).trim());
        ingestDurationStandalone   = Integer.parseInt(    prop.getProperty(INGEST_STANDALONE_DURATION).trim());
        ingestDurationEndDate      = LocalDate.parse(     prop.getProperty(INGEST_DURATION_END_DATE).trim());
        ingestTarget               = DBTargets.valueOf(   prop.getProperty(INGEST_TARGET).toUpperCase().trim());
        ingestTargetRecreate       = Boolean.parseBoolean(prop.getProperty(INGEST_TARGET_RECREATE).trim());
        ingestTargetSharedInstance = Boolean.parseBoolean(prop.getProperty(INGEST_SHARED_INSTANCE).trim());
        ingestThreads              = Integer.parseInt(    prop.getProperty(INGEST_THREADS).trim());

        //Queries
        queriesEnabled           = Boolean.parseBoolean(prop.getProperty(QUERIES_ENABLED).trim());
        queriesTarget            = DBTargets.valueOf(   prop.getProperty(QUERIES_TARGET).toUpperCase().trim().trim());
        queriesThreads           = Integer.parseInt(    prop.getProperty(QUERIES_THREADS).trim());
        queriesSharedInstance    = Boolean.parseBoolean(prop.getProperty(QUERIES_SHARED_INSTANCE).trim());
        queriesDuration          = Integer.parseInt(    prop.getProperty(QUERIES_DURATION).trim());
        queriesWarmup            = Integer.parseInt(    prop.getProperty(QUERIES_WARMUP).trim());
        queriesMaxCount          = Integer.parseInt(    prop.getProperty(QUERIES_MAX_COUNT).trim());
        queriesReportFrequency   = Integer.parseInt(    prop.getProperty(QUERIES_REPORT_FREQUENCY).trim());
        queriesEarliestValidDate = LocalDate.parse(     prop.getProperty(QUERIES_EARLIEST_VALID_DATE).trim());
        queriesWeightTotalClients= Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_TOTAL_CLIENTS).trim());
        queriesWeightFloorTotals = Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_FLOOR_TOTALS).trim());
        queriesWeightMaxForAP    = Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_MAX_FOR_AP).trim());
        queriesWeightAvgOccupancy= Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_AVG_OCCUPANCY).trim());
        queriesWeightKMeans      = Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_KMEANS).trim());
        queriesRngRangeDay       = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_DAY).trim());
        queriesRngRangeWeek      = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_WEEK).trim());
        queriesRngRangeMonth     = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_MONTH).trim());
        queriesRngRangeYear      = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_YEAR).trim());
        queriesIntervalMin       = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MIN).trim());
        queriesIntervalMax       = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MAX).trim());
        queriesIntervalMinKMeans = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MIN_KMEANS).trim());
        queriesIntervalMaxKMeans = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MAX_KMEANS).trim());
        queriesKMeansClusters    = Integer.parseInt(    prop.getProperty(QUERIES_KMEANS_CLUSTERS).trim());
        queriesKMeansIterations  = Integer.parseInt(    prop.getProperty(QUERIES_KMEANS_ITERATIONS).trim());
        queriesDateCommIntervalMilliseconds = Integer.parseInt(prop.getProperty(QUERIES_DATE_COMM).trim());

        //Debug
        debugCreatePrecomputedTables = Boolean.parseBoolean(prop.getProperty(DEBUG_CREATE_PRECOMPUTED_TABLES).trim());
        debugPrintSettings           = Boolean.parseBoolean(prop.getProperty(DEBUG_PRINT_ALL_SETTINGS).trim());
        debugSaveQueryResults        = Boolean.parseBoolean(prop.getProperty(DEBUG_SAVE_QUERY_RESULTS).trim());
        debugSaveQueryResultsPath    =                      prop.getProperty(DEBUG_SAVE_QUERY_RESULTS_PATH);
        debugSynchronizeRngState     = Boolean.parseBoolean(prop.getProperty(DEBUG_SYNCHRONIZE_RNG_STATE).trim());
        debugTruncateQueryTimestamps = Boolean.parseBoolean(prop.getProperty(DEBUG_TRUNCATE_QUERY_TIMESTAMPS).trim());
        debugReportQueryStatus       = Boolean.parseBoolean(prop.getProperty(DEBUG_REPORT_QUERY_STATUS).trim());
    }

    private String validateConfig(){
        if(logToCSV){
            if(!Paths.get(logToCSVPath).toFile().exists()) return LOG_TO_CSV_PATH + ": CSV folder path doesn't exist: " + Paths.get(logToCSVPath).toFile().getAbsolutePath();
        }

        // ---- Serialize ----
        if(serialize){
            if(!Paths.get(serializePath).toFile().exists()) return SERIALIZE_PATH + ": Serialize folder path doesn't exist: " + Paths.get(serializePath).toFile().getAbsolutePath();
        }
        if(!serialize){
            if(!generatorEnabled) return "Both serialization (" + SERIALIZE_ENABLED + ") and the generator (" + GENERATOR_ENABLED + ") are disabled. One or both must be enabled to create/load the data needed for ingestion and queries.";
        }

        if(ingestEnabled || generatorEnabled){
            // Serialization doesn't serialize the source-data that ingestion relies on, so still ensure that those paths are valid if either the generator or ingestion is enabled.
            if(!Paths.get(generatorIdmap).toFile().exists()) return GENERATOR_IDMAP + ": Path doesn't exist: " + Paths.get(generatorIdmap).toFile().getAbsolutePath();
            if(!Paths.get(generatorMapfolder).toFile().exists()) return GENERATOR_MAP_FOLDER + ": Folder path doesn't exist: " + Paths.get(generatorMapfolder).toFile().getAbsolutePath();
        }

        // ---- Generator ----
        if(generatorEnabled){
            if(!(generatorStartDate.isBefore(generatorEndDate) || generatorStartDate.isEqual(generatorEndDate))) return GENERATOR_START_DATE + ": Start date " + generatorStartDate + " must be before end date " + generatorEndDate + "(" + GENERATOR_END_DATE + ")";
            if(!(generatorOutputTargets.length > 0)) return "Generator enabled but no generator targets specified (" + GENERATOR_OUTPUT_TARGETS + ")";
        }

        if(generatorEnabled || ingestEnabled){
            if(!(generatorScale > 0.0)) return GENERATOR_SCALE + ": Scale must be > 0.0";
            if(!(generatorSeedSamplerate > 0)) return GENERATOR_SEED_SAMPLE_RATE + ": Seed sample rate must be > 0";
            if(!(generatorGenerationSamplerate > 0)) return GENERATOR_GENERATION_SAMPLE_RATE + ": Generation sample rate must be > 0";
            if(!(generatorJitter >= 0)) return GENERATOR_JITTER + ": Generation jitter must be >= 0";
            if(!(generatorGenerationSamplerate == generatorSeedSamplerate ||
                    (generatorGenerationSamplerate < generatorSeedSamplerate && generatorSeedSamplerate % generatorGenerationSamplerate == 0) ||
                    (generatorGenerationSamplerate > generatorSeedSamplerate && generatorGenerationSamplerate % generatorSeedSamplerate == 0))) return GENERATOR_SEED_SAMPLE_RATE + " and " + GENERATOR_GENERATION_SAMPLE_RATE + ": Seed sample rate and generation sample rate must be equal, or one must be evenly divisible by the other";
        }

        // ---- Ingest ----
        if(ingestEnabled){
            if(!(ingestThreads > 0)) return INGEST_THREADS + ": Ingest threads must be > 0";
            if(ingestThreads > 1 && schema == SchemaFormats.COLUMN) return INGEST_THREADS + ": Column-format does not support multiple ingest threads. (to simplify the implementation)";

            if(generatorEnabled && !ingestTargetRecreate){
                if(!(ingestStartDate.isAfter(generatorStartDate) || ingestStartDate.isEqual(generatorStartDate))) return INGEST_START_DATE + ": Ingest start date " + ingestStartDate + " must be equal/after start date " + generatorStartDate + "(" + GENERATOR_START_DATE + ")";
            }

            if(!queriesEnabled){
                if(!(ingestDurationEndDate.isAfter(ingestStartDate))) return "Ingest end-date (" + INGEST_DURATION_END_DATE + ") must be after start-date (" + INGEST_START_DATE + ") for ingestion to run without also running queries.";
            }
        }

        // ---- Queries ----
        if(queriesEnabled){
            if(queriesTarget == DBTargets.CSV) return "Unsupported query target 'CSV' (" + QUERIES_TARGET + ")";
            if(!(queriesThreads > 0)) return QUERIES_THREADS + ": Query threads must be > 0";
            if(!(queriesDuration > 0 || queriesMaxCount > 0)) return "Query-duration (" + QUERIES_DURATION + ") must be > 0 or max query count (" + QUERIES_MAX_COUNT + ") must be > 0";
            if(!(queriesWeightTotalClients >= 0)) return QUERIES_WEIGHT_TOTAL_CLIENTS + ": Query-weight for 'TotalClients' must be >= 0";
            if(!(queriesWeightFloorTotals >= 0)) return QUERIES_WEIGHT_FLOOR_TOTALS + ": Query-weight for 'FloorTotals' must be >= 0";
            if(!(queriesWeightMaxForAP >= 0)) return QUERIES_WEIGHT_MAX_FOR_AP + ": Query-weight for 'Max For AP' must be >= 0";
            if(!(queriesWeightAvgOccupancy >= 0)) return QUERIES_WEIGHT_AVG_OCCUPANCY + ": Query-weight for 'Avg Occupancy' must be >= 0";
            if(!(queriesWeightKMeans >= 0)) return QUERIES_WEIGHT_KMEANS + ": Query-weight for 'K-Means' must be >= 0";
            if(!(queriesIntervalMin >= 0)) return QUERIES_INTERVAL_MIN + ": Minimum query interval must be >= 0";
            if(!(queriesIntervalMax >= 0)) return QUERIES_INTERVAL_MAX + ": Maximum query interval must be >= 0";
            if(!(queriesIntervalMin <= queriesIntervalMax)) return QUERIES_INTERVAL_MIN + " and " + QUERIES_INTERVAL_MAX + ": Minimum query interval must be <= maximum query interval";
            if(!(queriesIntervalMinKMeans >= 0)) return QUERIES_INTERVAL_MIN_KMEANS + ": Minimum query interval must be >= 0";
            if(!(queriesIntervalMaxKMeans >= 0)) return QUERIES_INTERVAL_MAX_KMEANS + ": Maximum query interval must be >= 0";
            if(!(queriesIntervalMinKMeans <= queriesIntervalMaxKMeans)) return QUERIES_INTERVAL_MIN_KMEANS + " and " + QUERIES_INTERVAL_MAX_KMEANS + ": Minimum query interval must be <= maximum query interval";
            if(queriesKMeansClusters < 2 || queriesKMeansClusters > 113*generatorScale) return QUERIES_KMEANS_CLUSTERS + ": Cluster-amount must be greater than 1 and less than the approximate number of APs generated at the specified scale.";
            if(queriesKMeansIterations < 1) return QUERIES_KMEANS_ITERATIONS + ": Number of iterations for K-Means must be at least 1";
        }

        if(ingestEnabled && queriesEnabled){
            if(!(ingestTarget.equals(queriesTarget))) return "Ingestion and queries are both enabled, but have different targets (" + INGEST_TARGET + ", " + QUERIES_TARGET + ")";
        }

        // ---- Databases ----
        if(!(influxBatchsize > 0)) return INFLUX_BATCHSIZE + ": Batch size must be > 0";
        if(!(influxFlushtime > 0)) return INFLUX_BATCH_FLUSH_TIME + ": Flush time must be > 0";
        if(!(timescaleBatchSize > 0)) return TIMESCALE_BATCHSIZE + ": Batch size must be > 0";

        return null;
    }

    public void save(String filePath) throws IOException {
        try(OutputStream output = new FileOutputStream(filePath)){
            prop.store(output, "Config file for Benchmark");
        }
    }

    public Set<String> getUnknownKeysInInput(){
        Set<String> settingsInFile = prop.stringPropertyNames();
        Set<String> knownSettings = getSettings().keySet();
        // One of the above set-implementations does not support the 'removeAll' operation, so we manually populate a new set.
        Set<String> unknownSettings = new HashSet<>();
        for(String setting : settingsInFile){
            if(!knownSettings.contains(setting)){
                unknownSettings.add(setting);
            }
        }

        return unknownSettings;
    }

    @Override
    public int hashCode() {
        // The hashcode of the config-file is used as folder-names during some logging/debug-operations.
        // Folders starting with '-' are inconvenient to 'cd' into because the folder name can be
        // interpreted as a flag (unless it's escaped or prepended with '-- ').
        // We therefore grab the absolute value.
        return Math.abs(toString().hashCode());
    }

    @Override
    public String toString() {
        SortedMap<String, Object> settings = getSettings();

        StringBuilder sb = new StringBuilder();
        for(String key : settings.keySet()){
            Object val = settings.get(key);

            // Pretty-print the DBTargets-array, otherwise we end up with [Benchmark...@garbage]
            if(val instanceof DBTargets[]){
                DBTargets[] value = (DBTargets[]) val;
                String out = "";
                for(int i = 0; i < value.length; i++){
                    if(i == 0){
                        out += value[i];
                    } else {
                        out += "," + value[i];
                    }
                }
                val = out;
            }

            sb.append(key + " = " + val + "\n");
        }
        return sb.toString();
    }

    public SortedMap<String, Object> getSettings(){
        SortedMap<String, Object> settings = new TreeMap<>();
        settings.put(SEED, seed);
        settings.put(SCHEMA, schema);
        settings.put(LOG_TO_CSV, logToCSV);
        settings.put(LOG_TO_CSV_PATH, logToCSVPath);
        settings.put(CSV_FOLDER_PREFIX, csvFolderPrefix);
        settings.put(CSV_INCLUDE_HEADER, csvIncludeHeader);

        settings.put(SERIALIZE_ENABLED, serialize);
        settings.put(SERIALIZE_PATH, serializePath);

        settings.put(GENERATOR_ENABLED, generatorEnabled);
        settings.put(GENERATOR_IDMAP, generatorIdmap);
        settings.put(GENERATOR_GRANULARITY, generatorGranularity);
        settings.put(GENERATOR_JITTER, generatorJitter);
        settings.put(GENERATOR_SCALE, generatorScale);
        settings.put(GENERATOR_MAP_FOLDER, generatorMapfolder);
        settings.put(GENERATOR_SEED_SAMPLE_RATE, generatorSeedSamplerate);
        settings.put(GENERATOR_GENERATION_SAMPLE_RATE, generatorGenerationSamplerate);
        settings.put(GENERATOR_KEEP_FLOOR_ASSOCIATIONS, generatorKeepFloorAssociations);
        settings.put(GENERATOR_START_DATE, generatorStartDate);
        settings.put(GENERATOR_END_DATE, generatorEndDate);
        settings.put(GENERATOR_OUTPUT_TARGETS, generatorOutputTargets);
        settings.put(GENERATOR_OUTPUT_TO_DISK_TARGET, generatorToDiskTarget);

        settings.put(INFLUX_URL, influxUrl);
        settings.put(INFLUX_USERNAME, influxUsername);
        settings.put(INFLUX_PASSWORD, influxPassword);
        settings.put(INFLUX_DBNAME, influxDBName);
        settings.put(INFLUX_TABLE, influxTable);
        settings.put(INFLUX_BATCHSIZE, influxBatchsize);
        settings.put(INFLUX_BATCH_FLUSH_TIME, influxFlushtime);

        settings.put(TIMESCALE_HOST, timescaleHost);
        settings.put(TIMESCALE_USERNAME, timescaleUsername);
        settings.put(TIMESCALE_PASSWORD, timescalePassword);
        settings.put(TIMESCALE_DBNAME, timescaleDBName);
        settings.put(TIMESCALE_TABLE, timescaleTable);
        settings.put(TIMESCALE_BATCHSIZE, timescaleBatchSize);
        settings.put(TIMESCALE_REWRITE_BATCH, timescaleReWriteBatchedInserts);
        settings.put(TIMESCALE_CREATE_SECONDARY_INDEX, timescaleCreateSecondaryIndex);

        settings.put(KUDU_HOST, kuduMasters);
        settings.put(KUDU_TABLE, kuduTable);
        settings.put(KUDU_MAX_SUPPORTED_COLUMNS, kuduMaxColumns);
        settings.put(KUDU_BATCH_SIZE, kuduBatchSize);
        settings.put(KUDU_MUTATION_BUFFER_SPACE, kuduMutationBufferSpace);
        settings.put(KUDU_PARTITION_TYPE, kuduPartitionType);
        settings.put(KUDU_HASH_PARTITION_BUCKETS, kuduHashBuckets);
        settings.put(KUDU_RANGE_PARTITION_INTERVAL, kuduPartitionInterval);
        settings.put(KUDU_RANGE_PARTITION_PRECREATE_YEARS, kuduRangePrecreatedNumberOfYears);

        settings.put(INGEST_ENABLED, ingestEnabled);
        settings.put(INGEST_START_DATE, ingestStartDate);
        settings.put(INGEST_SPEED, ingestSpeed);
        settings.put(INGEST_REPORT_FREQUENCY, ingestReportFrequency);
        settings.put(INGEST_STANDALONE_DURATION, ingestDurationStandalone);
        settings.put(INGEST_DURATION_END_DATE, ingestDurationEndDate);
        settings.put(INGEST_TARGET, ingestTarget);
        settings.put(INGEST_TARGET_RECREATE, ingestTargetRecreate);
        settings.put(INGEST_SHARED_INSTANCE, ingestTargetSharedInstance);
        settings.put(INGEST_THREADS, ingestThreads);

        settings.put(QUERIES_ENABLED, queriesEnabled);
        settings.put(QUERIES_TARGET, queriesTarget);
        settings.put(QUERIES_THREADS, queriesThreads);
        settings.put(QUERIES_SHARED_INSTANCE, queriesSharedInstance);
        settings.put(QUERIES_DURATION, queriesDuration);
        settings.put(QUERIES_WARMUP, queriesWarmup);
        settings.put(QUERIES_MAX_COUNT, queriesMaxCount);
        settings.put(QUERIES_REPORT_FREQUENCY, queriesReportFrequency);
        settings.put(QUERIES_EARLIEST_VALID_DATE, queriesEarliestValidDate);
        settings.put(QUERIES_WEIGHT_TOTAL_CLIENTS, queriesWeightTotalClients);
        settings.put(QUERIES_WEIGHT_FLOOR_TOTALS, queriesWeightFloorTotals);
        settings.put(QUERIES_WEIGHT_MAX_FOR_AP, queriesWeightMaxForAP);
        settings.put(QUERIES_WEIGHT_AVG_OCCUPANCY, queriesWeightAvgOccupancy);
        settings.put(QUERIES_WEIGHT_KMEANS, queriesWeightKMeans);
        settings.put(QUERIES_RNG_RANGE_DAY, queriesRngRangeDay);
        settings.put(QUERIES_RNG_RANGE_WEEK, queriesRngRangeWeek);
        settings.put(QUERIES_RNG_RANGE_MONTH, queriesRngRangeMonth);
        settings.put(QUERIES_RNG_RANGE_YEAR, queriesRngRangeYear);
        settings.put(QUERIES_INTERVAL_MIN, queriesIntervalMin);
        settings.put(QUERIES_INTERVAL_MAX, queriesIntervalMax);
        settings.put(QUERIES_INTERVAL_MIN_KMEANS, queriesIntervalMinKMeans);
        settings.put(QUERIES_INTERVAL_MAX_KMEANS, queriesIntervalMaxKMeans);
        settings.put(QUERIES_KMEANS_CLUSTERS, queriesKMeansClusters);
        settings.put(QUERIES_KMEANS_ITERATIONS, queriesKMeansIterations);
        settings.put(QUERIES_DATE_COMM, queriesDateCommIntervalMilliseconds);

        settings.put(DEBUG_CREATE_PRECOMPUTED_TABLES, debugCreatePrecomputedTables);
        settings.put(DEBUG_PRINT_ALL_SETTINGS, debugPrintSettings);
        settings.put(DEBUG_SAVE_QUERY_RESULTS, debugSaveQueryResults);
        settings.put(DEBUG_SAVE_QUERY_RESULTS_PATH, debugSaveQueryResultsPath);
        settings.put(DEBUG_SYNCHRONIZE_RNG_STATE, debugSynchronizeRngState);
        settings.put(DEBUG_TRUNCATE_QUERY_TIMESTAMPS, debugTruncateQueryTimestamps);
        settings.put(DEBUG_REPORT_QUERY_STATUS, debugReportQueryStatus);

        return settings;
    }

    public boolean isValidConfig() {
        return validated;
    }

    public String getValidationError(){
        return validationError;
    }

    public boolean isGeneratorEnabled() {
        return generatorEnabled;
    }

    public boolean isQueryingEnabled() {
        return queriesEnabled;
    }

    public boolean isIngestionEnabled() {
        return ingestEnabled;
    }

    public double getGeneratorScale() {
        return generatorScale;
    }

    public int getSeed() {
        return seed;
    }

    public String getGeneratorIdmap() {
        return generatorIdmap;
    }

    public String getGeneratorMapfolder() {
        return generatorMapfolder;
    }

    public int getGeneratorSeedSamplerate() {
        return generatorSeedSamplerate;
    }

    public int getGeneratorGenerationSamplerate() {
        return generatorGenerationSamplerate;
    }

    public LocalDate getGeneratorStartDate() {
        return generatorStartDate;
    }

    public LocalDate getGeneratorEndDate() {
        return generatorEndDate;
    }

    public DBTargets[] saveGeneratedDataTargets() {
        return generatorOutputTargets;
    }

    public String getGeneratorDiskTarget() {
        return generatorToDiskTarget;
    }

    public String getInfluxUrl() {
        return influxUrl;
    }

    public String getInfluxUsername() {
        return influxUsername;
    }

    public String getInfluxPassword() {
        return influxPassword;
    }

    public String getInfluxDBName() {
        return influxDBName;
    }

    public String getInfluxTable() {
        return influxTable;
    }

    public boolean keepFloorAssociationsForGenerator() {
        return generatorKeepFloorAssociations;
    }

    public boolean DEBUG_createPrecomputedTables() {
        return debugCreatePrecomputedTables;
    }

    public boolean DEBUG_printSettings() {
        return debugPrintSettings;
    }

    public boolean DEBUG_saveQueryResults() {
        return debugSaveQueryResults;
    }

    public String DEBUG_saveQueryResultsPath() {
        return debugSaveQueryResultsPath;
    }

    public boolean DEBUG_synchronizeRngState() {
        return debugSynchronizeRngState;
    }

    public boolean DEBUG_truncateQueryTimestamps() {
        return debugTruncateQueryTimestamps;
    }

    public boolean DEBUG_reportQueryStatus(){
        return debugReportQueryStatus;
    }

    public boolean doSerialization() {
        return serialize;
    }

    public String getSerializationPath() {
        return serializePath;
    }

    public LocalDate getIngestStartDate() {
        return ingestStartDate;
    }

    public LocalDate getIngestEndDate() {
        return ingestDurationEndDate;
    }

    public int getIngestSpeed() {
        return ingestSpeed;
    }

    public int getIngestReportFrequency() {
        return ingestReportFrequency;
    }

    public int getQueriesDuration() {
        return queriesDuration;
    }

    public int getQueriesWarmupDuration() {
        return queriesWarmup;
    }

    public int getQueriesWeightTotalClients() {
        return queriesWeightTotalClients;
    }

    public int getQueriesWeightFloorTotals() {
        return queriesWeightFloorTotals;
    }

    public int getQueriesWeightAvgOccupancy() {
        return queriesWeightAvgOccupancy;
    }

    public int getQueriesWeightKMeans() {
        return queriesWeightKMeans;
    }

    public LocalDate getQueriesEarliestValidDate() {
        return queriesEarliestValidDate;
    }

    public double getQueriesRngRangeDay() {
        return queriesRngRangeDay;
    }

    public double getQueriesRngRangeWeek() {
        return queriesRngRangeWeek;
    }

    public double getQueriesRngRangeMonth() {
        return queriesRngRangeMonth;
    }

    public double getQueriesRngRangeYear() {
        return queriesRngRangeYear;
    }

    public int getQueriesIntervalMin() {
        return queriesIntervalMin;
    }

    public int getQueriesIntervalMax() {
        return queriesIntervalMax;
    }

    public int getQueriesIntervalMinKMeans() {
        return queriesIntervalMinKMeans;
    }

    public int getQueriesIntervalMaxKMeans() {
        return queriesIntervalMaxKMeans;
    }

    public int getQueriesKMeansClusters() {
        return queriesKMeansClusters;
    }

    public int getQueriesKMeansIterations() {
        return queriesKMeansIterations;
    }

    public int getIngestThreadCount() {
        return ingestThreads;
    }

    public int getQueriesThreadCount() {
        return queriesThreads;
    }

    public int getIngestionStandaloneDuration() {
        return ingestDurationStandalone;
    }

    public int getMaxQueryCount() {
        return queriesMaxCount;
    }

    public int getQueriesReportingFrequency() {
        return queriesReportFrequency;
    }

    public DBTargets getIngestTarget() {
        return ingestTarget;
    }

    public DBTargets getQueriesTarget() {
        return queriesTarget;
    }

    public boolean useSharedQueriesInstance() {
        return queriesSharedInstance;
    }

    public boolean useSharedIngestInstance() {
        return ingestTargetSharedInstance;
    }

    public boolean recreateIngestTarget() {
        return ingestTargetRecreate;
    }

    public String getTimescaleHost() {
        return timescaleHost;
    }

    public String getTimescaleUsername() {
        return timescaleUsername;
    }

    public String getTimescalePassword() {
        return timescalePassword;
    }

    public String getTimescaleDBName() {
        return timescaleDBName;
    }

    public String getTimescaleTable() {
        return timescaleTable;
    }

    public Integer getTimescaleBatchSize() {
        return timescaleBatchSize;
    }

    public boolean reWriteBatchedTimescaleInserts() {
        return timescaleReWriteBatchedInserts;
    }

    public boolean getTimescaleCreateSecondaryIndex(){
        return timescaleCreateSecondaryIndex;
    }

    public int getInfluxBatchsize() {
        return influxBatchsize;
    }

    public int getInfluxFlushtime() {
        return influxFlushtime;
    }

    public Granularity getGeneratorGranularity() {
        return generatorGranularity;
    }

    public int getQueriesWeightMaxForAP() {
        return queriesWeightMaxForAP;
    }

    public int getGeneratorJitter() {
        return generatorJitter;
    }

    public int getQueryDateCommunicationIntervalInMillisec() {
        return queriesDateCommIntervalMilliseconds;
    }

    public boolean doDateCommunicationByQueryingDatabase() {
        return queriesDateCommIntervalMilliseconds >= 0;
    }

    public SchemaFormats getSchema(){
        return schema;
    }

    public String getKuduMasters(){
        return kuduMasters;
    }

    public String getKuduTable(){
        return kuduTable;
    }

    public int getKuduMaxColumns(){
        return kuduMaxColumns;
    }

    public int getKuduBatchSize(){
        return kuduBatchSize;
    }

    public int getKuduMutationBufferSpace(){
        return kuduMutationBufferSpace;
    }

    public KuduPartitionInterval getKuduPartitionInterval() {
        return kuduPartitionInterval;
    }

    public int getKuduHashBuckets(){
        return kuduHashBuckets;
    }

    public KuduPartitionType getKuduPartitionType(){
        return kuduPartitionType;
    }

    public int getKuduRangePrecreatedNumberOfYears(){
        return kuduRangePrecreatedNumberOfYears;
    }

    public boolean doLoggingToCSV(){
        return logToCSV;
    }

    public String getCSVLogPath(){
        return logToCSVPath;
    }

    public String getCsvFolderPrefix(){
        return csvFolderPrefix.trim().toLowerCase().equals("none") ? "" : csvFolderPrefix;
    }

    public boolean includeCsvHeaderInOutput(){
        return csvIncludeHeader;
    }
}
