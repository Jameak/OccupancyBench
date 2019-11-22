package Benchmark.Config;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    private int seed;

    /**
     * Type: Boolean
     * Enables de-/serialization of generated floors and the state of the RNG after initial data generation.
     *
     * If both serialization and the generator is enabled, floor information and the RNG will be generated and then serialized.
     * If serialization is enabled and the generator is disabled, the program will attempt to deserialize this data instead.
     */
    private static final String SERIALIZE_ENABLED = "serialization.enabled";
    /**
     * Type: String
     * Path of the folder to de-/serialize data into/from.
     */
    private static final String SERIALIZE_PATH    = "serialization.path";
    private boolean   serialize;
    private String    serializePath;

    /**
     * Type: Boolean
     * Controls whether floor information and initial data is generated.
     * Any generated initial data is added to the database.
     *
     * The generator is also used during ingestion, so set the generator-config settings accordingly even if
     * initial data generation is turned off.
     */
    private static final String GENERATOR_ENABLED                 = "generator.enabled";
    /**
     * Type: Double
     * The scaling applied to the generated data. A value of 1.0 resembles the amount of data generated in the ITU system.
     * Changing the scaling affects the number of floors and access-points that are generated, as well as the total number
     * of clients distributed among them.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_SCALE                   = "generator.data.scale";
    /**
     * Type: A single accepted value. Accepted values are: NANOSECOND, MILLISECOND, SECOND, MINUTE
     * The granularity of the time-stamps inserted into the target database.
     * Data will be generated with nanosecond-granularity and then truncated to this value.
     *
     * Note that a granularity that is coarser than the specified {@code GENERATOR_GENERATION_INTERVAL} will result in writes
     * to the database that cannot be distinguished from each other because their timestamps will be truncated to the same value.
     *
     * Note: InfluxDB supports nanosecond-granularity (or coarser). TimescaleDB supports millisecond-granularity
     *       (or coarser), so values inserted into TimescaleDB will be truncated to milliseconds or coarser,
     *       even if nanosecond-granularity is specified.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_GRANULARITY             = "generator.data.granularity";
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
    /**
     * Type: String
     * The path to the mapping-file between ITU AP-names and their assigned ID, as used in the probability-map
     * created by the python script.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_IDMAP                   = "generator.data.idmap";
    /**
     * Type: String
     * The path to the folder containing the probability-map files as created by the python script.
     * This folder should ONLY contain probability-map files and nothing else.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_MAP_FOLDER              = "generator.data.folder";
    /**
     * Type: Integer
     * The sampling rate in seconds for the AP-readings in the probability-map files.
     * Value must be evenly divisible by {@code GENERATOR_GENERATION_INTERVAL}.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_SOURCE_INTERVAL         = "generator.data.sourceinterval";
    /**
     * Type: Integer
     * The sampling rate in seconds for the AP-readings to be generated by the generator.
     * Value must be evenly divisible by {@code GENERATOR_SOURCE_INTERVAL}.
     *
     * Also used during ingest-generation.
     */
    private static final String GENERATOR_GENERATION_INTERVAL     = "generator.data.generationinterval";
    /**
     * Type: Boolean
     * Controls whether the mapping of data from ITU-APs to fake, generated APs should preserve floor-numbers.
     * That is, whether an ITU-AP from floor 3 must be assigned to a generated AP on the generated floor 3 or whether
     * it can be assigned to any floor.
     */
    private static final String GENERATOR_KEEP_FLOOR_ASSOCIATIONS = "generator.data.keepfloorassociations";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The start date for initial data generation. Inclusive.
     */
    private static final String GENERATOR_START_DATE              = "generator.data.startdate";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The end date for initial data generation. Exclusive.
     */
    private static final String GENERATOR_END_DATE                = "generator.data.enddate";
    /**
     * Type: Comma-separated string of accepted values. Accepted values are: FILE, INFLUX, TIMESCALE
     * The outputs to add generated data to. If multiple targets are specified, all targets receive data as it is generated.
     */
    private static final String GENERATOR_OUTPUT_TARGETS          = "generator.output.targets";
    /**
     * Type: String
     * If FILE is specified in {@code GENERATOR_OUTPUT_TARGETS} then this is the path of the file that the generated
     * data is written to.
     */
    private static final String GENERATOR_OUTPUT_TO_DISK_TARGET   = "generator.output.filepath";
    private boolean   generatorEnabled;
    private double    generatorScale;
    private Granularity generatorGranularity;
    private int       generatorJitter;
    private String    generatorIdmap;
    private String    generatorMapfolder;
    private int       generatorSourceInterval;
    private int       generatorGenerationInterval;
    private boolean   generatorKeepFloorAssociations;
    private LocalDate generatorStartDate;
    private LocalDate generatorEndDate;
    private Target[]  generatorOutputTargets;
    private String    generatorToDiskTarget;

    /**
     * Type: String
     * The url of the Influx-database to connect to. Must include the port-number.
     */
    private static final String INFLUX_URL      = "influx.url";
    /**
     * Type: String
     * The username of the Influx user to use.
     */
    private static final String INFLUX_USERNAME = "influx.username";
    /**
     * Type: String
     * The password of the specified Influx user.
     */
    private static final String INFLUX_PASSWORD = "influx.password";
    /**
     * Type: String
     * The name of the database to generate/query.
     */
    private static final String INFLUX_DBNAME   = "influx.dbname";
    /**
     * Type: String
     * The name of the Influx-measurement to generate/query.
     */
    private static final String INFLUX_TABLE     = "influx.table";
    /**
     * Type: Integer
     * The max number of inserts to batch together during generation/ingestion.
     * A batch-write is issued if either the batch-size or flush-time is reached
     */
    private static final String INFLUX_BATCHSIZE = "influx.batch.size";
    /**
     * Type: Integer
     * The max number of milliseconds between batch-writes.
     * A batch-write is issued if either the batch-size or flush-time is reached
     */
    private static final String INFLUX_BATCH_FLUSH_TIME = "influx.batch.flushtime";
    private String  influxUrl;
    private String  influxUsername;
    private String  influxPassword;
    private String  influxDBName;
    private String  influxTable;
    private int     influxBatchsize;
    private int     influxFlushtime;

    /**
     * Type: String
     * The host of the Timescale-database to connect to. May include a port-number.
     */
    private static final String TIMESCALE_HOST      = "timescale.host";
    /**
     * Type: String
     * The username of the Timescale user to use.
     */
    private static final String TIMESCALE_USERNAME = "timescale.username";
    /**
     * Type: String
     * The password of the specified Timescale user.
     */
    private static final String TIMESCALE_PASSWORD = "timescale.password";
    /**
     * Type: String
     * The name of the database to generate/query.
     * The database must have been created already.
     */
    private static final String TIMESCALE_DBNAME   = "timescale.dbname";
    /**
     * Type: String
     * The name of the Timescale-table to generate/query.
     */
    private static final String TIMESCALE_TABLE    = "timescale.table";
    /**
     * Type: Integer
     * The number of inserts to batch together during generation/ingestion.
     */
    private static final String TIMESCALE_BATCHSIZE     = "timescale.batchsize";
    /**
     * Type: Boolean
     * Controls whether the property "reWriteBatchedInserts" is included in the Timescale connection string.
     */
    private static final String TIMESCALE_REWRITE_BATCH = "timescale.rewritebatchedinserts";
    private String  timescaleHost;
    private String  timescaleUsername;
    private String  timescalePassword;
    private String  timescaleDBName;
    private String  timescaleTable;
    private Integer timescaleBatchSize;
    private boolean timescaleReWriteBatchedInserts;

    /**
     * Type: Boolean
     * Controls whether ingestion is enabled.
     */
    private static final String INGEST_ENABLED             = "ingest.enabled";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The first date to generate data for.
     */
    private static final String INGEST_START_DATE          = "ingest.startdate";
    /**
     * Type: Integer
     * The desired number of entries for each ingestion-thread to generate per second.
     * The desired number of entries are spread out over each second with a variable delay to reach the desired speed.
     * A value <= 0 will not limit the ingestion-speed.
     */
    private static final String INGEST_SPEED               = "ingest.speed";
    /**
     * Type: Integer
     * The number of seconds between ingestion reporting the current ingest-speed.
     * A value of <= 0 will make ingestion not report any intermediate values.
     */
    private static final String INGEST_REPORT_FREQUENCY    = "ingest.reportfrequency";
    /**
     * Type: Integer
     * The number of seconds to run ingestion for, if ingestion is run on its own.
     *
     * If queries are enabled, then {@code QUERIES_DURATION} controls how long to run ingestion for and this value is ignored.
     * If both this and {@code INGEST_DURATION_END_DATE} is specified then whichever duration is first reached will
     * terminate ingestion.
     */
    private static final String INGEST_STANDALONE_DURATION = "ingest.duration.time";
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
    /**
     * Type: A single accepted value. Accepted values are: INFLUX, TIMESCALE
     * The target to write generated ingest-data to.
     */
    private static final String INGEST_TARGET              = "ingest.target";
    /**
     * Type: Boolean
     * Controls whether the configured target should be recreated (dropped, then created) before ingestion begins.
     */
    private static final String INGEST_TARGET_RECREATE     = "ingest.target.recreate";
    /**
     * Type: Boolean
     * Controls whether the ingest-threads share a single Target-instance or whether they each get one instance.
     *
     * A single instance allows sharing of handles and resources, but no external synchronization is provided.
     * The Target-implementation for the specified {@code INGEST_TARGET} must be thread-safe if this is enabled.
     */
    private static final String INGEST_SHARED_INSTANCE     = "ingest.target.sharedinstance";
    /**
     * Type: Integer
     * The number of threads to create for ingestion. A dedicated ingest-threadpool is created with this many threads,
     * and this many ingest-tasks are then submitted to the pool.
     */
    private static final String INGEST_THREADS             = "ingest.threads";
    private boolean   ingestEnabled;
    private LocalDate ingestStartDate;
    private int       ingestSpeed;
    private int       ingestReportFrequency;
    private int       ingestDurationStandalone;
    private LocalDate ingestDurationEndDate;
    private Target    ingestTarget;
    private boolean   ingestTargetRecreate;
    private boolean   ingestTargetSharedInstance;
    private int       ingestThreads;

    /**
     * Type: Boolean
     * Controls whether queries are enabled.
     */
    private static final String QUERIES_ENABLED                  = "queries.enabled";
    /**
     * Type: A single accepted value. Accepted values are: INFLUX, TIMESCALE
     * The target to run queries against.
     */
    private static final String QUERIES_TARGET                   = "queries.target";
    /**
     * Type: Boolean
     * Controls whether the query-threads share a single Queries-instance or whether they each get one instance.
     *
     * A single instance allows sharing of handles and resources, but no external synchronization is provided.
     * The Queries-implementation for the specified {@code QUERIES_TARGET} must be thread-safe if this is enabled.
     */
    private static final String QUERIES_SHARED_INSTANCE          = "queries.target.sharedinstance";
    /**
     * Type: Integer
     * The number of threads to create for querying. A dedicated query-threadpool is created with this many threads,
     * and this many query-tasks are then submitted to the pool.
     */
    private static final String QUERIES_THREADS                  = "queries.threads";
    /**
     * Type: Integer
     * The number of seconds to run the queries for before stopping.
     */
    private static final String QUERIES_DURATION                 = "queries.duration.time";
    /**
     * Type: Integer
     * The number of seconds to do query-warmup for. Warmup runs the exact same code as normal queries, but doesn't
     * collect statistics.
     */
    private static final String QUERIES_WARMUP                   = "queries.duration.warmup";
    /**
     * Type: Integer
     * The number of queries to run before stopping. Only considered if {@code QUERIES_DURATION} is <= 0.
     */
    private static final String QUERIES_MAX_COUNT                = "queries.duration.count";
    /**
     * Type: Integer
     * The number of seconds between queries reporting query-statistics during query-execution.
     * In-progress query statistics will report the average query-speeds since the previous report, whereas the
     * final report prints values over the entire query-duration.
     *
     * A value of <= 0 will make querying not report any intermediate values.
     */
    private static final String QUERIES_REPORT_FREQUENCY         = "queries.reportfrequency";
    /**
     * Type: LocalDate (YYYY-MM-DD)
     * The earliest possible date for queries to ask for.
     * Set this value to the first date in the database if the entire data-set is under consideration,
     * or set it to a later date if values earlier than this date should never be queried.
     */
    private static final String QUERIES_EARLIEST_VALID_DATE      = "queries.earliestdate";
    /**
     * Type: Integer
     * The weight of the 'Total Clients' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     *
     * Example: If 3 queries have the weights 1,1,2 then queries #1 and #2 have a 25% chance of being selected,
     *          while query #3 has a 50% chance of being selected.
     */
    private static final String QUERIES_WEIGHT_TOTAL_CLIENTS     = "queries.weight.totalclients";
    /**
     * Type: Integer
     * The weight of the 'Floor Totals' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_FLOOR_TOTALS      = "queries.weight.floortotals";
    /**
     * Type: Integer
     * The weight of the 'Max for AP' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_MAX_FOR_AP      = "queries.weight.maxforap";
    /**
     * Type: Integer
     * The weight of the 'Avg occupancy' query when a random query is selected.
     * A weight of 0 will prevent this query from being selected.
     */
    private static final String QUERIES_WEIGHT_AVG_OCCUPANCY      = "queries.weight.avgoccupancy";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value, then the time-interval
     * used in the query will at-most range over 24 hours, and will be at-most 24 hours old.
     *
     * That is, if P(X < {@code QUERIES_RNG_RANGE_DAY}) then this setting is used for the time-component of queries.
     */
    private static final String QUERIES_RNG_RANGE_DAY            = "queries.range.day";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value and greater than {@code QUERIES_RNG_RANGE_DAY},
     * then the time-interval used in the query will at-most range over 7 days and will be at-most 7 days old.
     *
     * That is, if P({@code QUERIES_RNG_RANGE_DAY} <= X < {@code QUERIES_RNG_RANGE_WEEK}) then this setting is used
     * for the time-component of queries.
     */
    private static final String QUERIES_RNG_RANGE_WEEK           = "queries.range.week";
    /**
     * Type: Double
     * If the random number drawn from X~uniform(0,1) is less than this value and greater than {@code QUERIES_RNG_RANGE_WEEK},
     * then the time-interval used in the query will at-most range over 30 days, and will be at-most 30 days old.
     *
     * That is, if P({@code QUERIES_RNG_RANGE_WEEK} <= X < {@code QUERIES_RNG_RANGE_MONTH}) then this setting is used
     * for the time-component of queries.
     */
    private static final String QUERIES_RNG_RANGE_MONTH          = "queries.range.month";
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
    /**
     * Type: Integer
     * The minimal length of a time-span to query for, in seconds. Spans shorter than this duration will be extended,
     * biased towards new values.
     */
    private static final String QUERIES_INTERVAL_MIN             = "queries.interval.min";
    /**
     * Type: Integer
     * The maximum length of a time-span to query for, in seconds. Spans longer than this duration will be shortened,
     * biased towards old values.
     */
    private static final String QUERIES_INTERVAL_MAX             = "queries.interval.max";
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
    private boolean   queriesEnabled;
    private Target    queriesTarget;
    private int       queriesThreads;
    private boolean   queriesSharedInstance;
    private int       queriesDuration;
    private int       queriesWarmup;
    private int       queriesMaxCount;
    private int       queriesReportFrequency;
    private LocalDate queriesEarliestValidDate;
    private int       queriesWeightTotalClients;
    private int       queriesWeightFloorTotals;
    private int       queriesWeightMaxForAP;
    private int       queriesWeightAvgOccupancy;
    private double    queriesRngRangeDay;
    private double    queriesRngRangeWeek;
    private double    queriesRngRangeMonth;
    private double    queriesRngRangeYear;
    private int       queriesIntervalMin;
    private int       queriesIntervalMax;
    private int       queriesDateCommIntervalMilliseconds;

    /**
     * Type: Boolean
     * Controls whether to generate tables containing pre-computed data for the purposes of debugging and graphing.
     */
    private static final String DEBUG_CREATE_PRECOMPUTED_TABLES = "debug.createprecomputedtables";
    /**
     * Type: Boolean
     * If enabled, all settings are printed at program start. Both those specified in the loaded config and the default
     * values are printed.
     */
    private static final String DEBUG_PRINT_ALL_SETTINGS = "debug.printallsettings";
    private boolean debugCreatePrecomputedTables;
    private boolean debugPrintSettings;

    private final Properties prop = new Properties();
    private boolean validated;
    private String validationError = "NO ERROR";

    public enum Target{
        FILE, INFLUX, TIMESCALE
    }
    
    public enum Granularity{
        NANOSECOND, MILLISECOND, SECOND, MINUTE;
        public TimeUnit toTimeUnit(){
            switch (this){
                case NANOSECOND:
                    return TimeUnit.NANOSECONDS;
                case MILLISECOND:
                    return TimeUnit.MILLISECONDS;
                case SECOND:
                    return TimeUnit.SECONDS;
                case MINUTE:
                    return TimeUnit.MINUTES;
                default:
                    throw new IllegalStateException("Unexpected value: " + this);
            }
        }
    }

    private ConfigFile(){ }

    public static ConfigFile load(String filePath) throws IOException {
        ConfigFile config = new ConfigFile();
        try(InputStream input = new FileInputStream(filePath)){
            config.prop.load(input);
        }

        config.parseProps();

        String error = config.validateConfig();
        assert error == null : error;
        if(error == null) config.validated = true;
        else config.validationError = error;

        return config;
    }

    public static ConfigFile defaultConfig(){
        ConfigFile config = new ConfigFile();

        //Benchmark
        config.prop.setProperty(SEED, "1234");

        //Serialization
        config.prop.setProperty(SERIALIZE_ENABLED, "false");
        config.prop.setProperty(SERIALIZE_PATH, "FOLDER PATH");

        //Generator
        config.prop.setProperty(GENERATOR_ENABLED, "true");
        config.prop.setProperty(GENERATOR_SCALE, "1.0");
        config.prop.setProperty(GENERATOR_GRANULARITY, "nanosecond");
        config.prop.setProperty(GENERATOR_JITTER, "100");
        config.prop.setProperty(GENERATOR_IDMAP, "FILE PATH");
        config.prop.setProperty(GENERATOR_MAP_FOLDER, "FOLDER PATH");
        config.prop.setProperty(GENERATOR_SOURCE_INTERVAL, "60");
        config.prop.setProperty(GENERATOR_GENERATION_INTERVAL, "60");
        config.prop.setProperty(GENERATOR_KEEP_FLOOR_ASSOCIATIONS, "true");
        config.prop.setProperty(GENERATOR_START_DATE, "2019-01-01");
        config.prop.setProperty(GENERATOR_END_DATE, "2019-04-01");
        config.prop.setProperty(GENERATOR_OUTPUT_TARGETS, "influx");
        config.prop.setProperty(GENERATOR_OUTPUT_TO_DISK_TARGET, "TARGET FILE PATH");

        //Influx
        config.prop.setProperty(INFLUX_URL, "localhost:8086");
        config.prop.setProperty(INFLUX_USERNAME, "USERNAME");
        config.prop.setProperty(INFLUX_PASSWORD, "PASSWORD");
        config.prop.setProperty(INFLUX_DBNAME, "benchmark");
        config.prop.setProperty(INFLUX_TABLE, "generated");
        config.prop.setProperty(INFLUX_BATCHSIZE, "10000");
        config.prop.setProperty(INFLUX_BATCH_FLUSH_TIME, "1000");

        //Timescale
        config.prop.setProperty(TIMESCALE_HOST, "localhost:5432");
        config.prop.setProperty(TIMESCALE_USERNAME, "USERNAME");
        config.prop.setProperty(TIMESCALE_PASSWORD, "PASSWORD");
        config.prop.setProperty(TIMESCALE_DBNAME, "benchmark");
        config.prop.setProperty(TIMESCALE_TABLE, "generated");
        config.prop.setProperty(TIMESCALE_BATCHSIZE, "10000");
        config.prop.setProperty(TIMESCALE_REWRITE_BATCH, "true");

        //Ingest
        config.prop.setProperty(INGEST_ENABLED, "true");
        config.prop.setProperty(INGEST_START_DATE, "2019-04-01");
        config.prop.setProperty(INGEST_SPEED, "-1");
        config.prop.setProperty(INGEST_REPORT_FREQUENCY, "-1");
        config.prop.setProperty(INGEST_STANDALONE_DURATION, "-1");
        config.prop.setProperty(INGEST_DURATION_END_DATE, "9999-12-31");
        config.prop.setProperty(INGEST_TARGET, "influx");
        config.prop.setProperty(INGEST_TARGET_RECREATE, "false");
        config.prop.setProperty(INGEST_SHARED_INSTANCE, "false");
        config.prop.setProperty(INGEST_THREADS, "1");

        //Queries
        config.prop.setProperty(QUERIES_ENABLED, "true");
        config.prop.setProperty(QUERIES_TARGET, "influx");
        config.prop.setProperty(QUERIES_THREADS, "1");
        config.prop.setProperty(QUERIES_SHARED_INSTANCE, "false");
        config.prop.setProperty(QUERIES_DURATION, "60");
        config.prop.setProperty(QUERIES_WARMUP, "5");
        config.prop.setProperty(QUERIES_MAX_COUNT, "-1");
        config.prop.setProperty(QUERIES_REPORT_FREQUENCY, "20");
        config.prop.setProperty(QUERIES_EARLIEST_VALID_DATE, "2019-01-01");
        config.prop.setProperty(QUERIES_WEIGHT_TOTAL_CLIENTS, "1");
        config.prop.setProperty(QUERIES_WEIGHT_FLOOR_TOTALS, "1");
        config.prop.setProperty(QUERIES_WEIGHT_MAX_FOR_AP, "2");
        config.prop.setProperty(QUERIES_WEIGHT_AVG_OCCUPANCY, "1");
        config.prop.setProperty(QUERIES_RNG_RANGE_DAY  , "0.4");
        config.prop.setProperty(QUERIES_RNG_RANGE_WEEK , "0.7");
        config.prop.setProperty(QUERIES_RNG_RANGE_MONTH, "0.9");
        config.prop.setProperty(QUERIES_RNG_RANGE_YEAR , "0.95");
        config.prop.setProperty(QUERIES_INTERVAL_MIN   , "21600"); // 6 hours in seconds
        config.prop.setProperty(QUERIES_INTERVAL_MAX   , "7776000"); // 90 days in seconds
        config.prop.setProperty(QUERIES_DATE_COMM      , "500");

        //Debug
        config.prop.setProperty(DEBUG_CREATE_PRECOMPUTED_TABLES, "false");
        config.prop.setProperty(DEBUG_PRINT_ALL_SETTINGS, "false");

        config.parseProps();
        return config;
    }

    private void parseProps(){
        //Benchmark
        seed             = Integer.parseInt(    prop.getProperty(SEED, "" + new Random().nextInt(10000)));

        //Serialization
        serialize        = Boolean.parseBoolean(prop.getProperty(SERIALIZE_ENABLED, "false"));
        serializePath    =                      prop.getProperty(SERIALIZE_PATH);

        //Generator
        generatorEnabled               = Boolean.parseBoolean(prop.getProperty(GENERATOR_ENABLED, "true"));
        generatorIdmap                 =                      prop.getProperty(GENERATOR_IDMAP);
        generatorGranularity           = Granularity.valueOf( prop.getProperty(GENERATOR_GRANULARITY, "nanosecond").toUpperCase().trim());
        generatorJitter                = Integer.parseInt(    prop.getProperty(GENERATOR_JITTER, "100"));
        generatorScale                 = Double.parseDouble(  prop.getProperty(GENERATOR_SCALE, "1.0"));
        generatorMapfolder             =                      prop.getProperty(GENERATOR_MAP_FOLDER);
        generatorSourceInterval        = Integer.parseInt(    prop.getProperty(GENERATOR_SOURCE_INTERVAL, "60"));
        generatorGenerationInterval    = Integer.parseInt(    prop.getProperty(GENERATOR_GENERATION_INTERVAL, "60"));
        generatorKeepFloorAssociations = Boolean.parseBoolean(prop.getProperty(GENERATOR_KEEP_FLOOR_ASSOCIATIONS, "true"));
        generatorStartDate             = LocalDate.parse(     prop.getProperty(GENERATOR_START_DATE, "2019-01-01"));
        generatorEndDate               = LocalDate.parse(     prop.getProperty(GENERATOR_END_DATE, "2019-04-01"));
        generatorOutputTargets         = Arrays.stream(       prop.getProperty(GENERATOR_OUTPUT_TARGETS, "influx").split(","))
                .map(String::toUpperCase).map(String::trim).map(Target::valueOf).toArray(Target[]::new);
        generatorToDiskTarget          =                      prop.getProperty(GENERATOR_OUTPUT_TO_DISK_TARGET);

        //Influx
        influxUrl       = "http://" +      prop.getProperty(INFLUX_URL, "localhost:8086");
        influxUsername  =                  prop.getProperty(INFLUX_USERNAME);
        influxPassword  =                  prop.getProperty(INFLUX_PASSWORD);
        influxDBName    =                  prop.getProperty(INFLUX_DBNAME, "benchmark");
        influxTable     =                  prop.getProperty(INFLUX_TABLE, "generated");
        influxBatchsize = Integer.parseInt(prop.getProperty(INFLUX_BATCHSIZE, "10000"));
        influxFlushtime = Integer.parseInt(prop.getProperty(INFLUX_BATCH_FLUSH_TIME, "1000"));

        //Timescale
        timescaleHost                  =                      prop.getProperty(TIMESCALE_HOST, "localhost:5432");
        timescaleUsername              =                      prop.getProperty(TIMESCALE_USERNAME);
        timescalePassword              =                      prop.getProperty(TIMESCALE_PASSWORD);
        timescaleDBName                =                      prop.getProperty(TIMESCALE_DBNAME, "benchmark");
        timescaleTable                 =                      prop.getProperty(TIMESCALE_TABLE, "generated");
        timescaleBatchSize             = Integer.parseInt(    prop.getProperty(TIMESCALE_BATCHSIZE, "10000"));
        timescaleReWriteBatchedInserts = Boolean.parseBoolean(prop.getProperty(TIMESCALE_REWRITE_BATCH, "true"));

        //Ingest
        ingestEnabled              = Boolean.parseBoolean(prop.getProperty(INGEST_ENABLED, "true"));
        ingestStartDate            = LocalDate.parse(     prop.getProperty(INGEST_START_DATE, "2019-04-01"));
        ingestSpeed                = Integer.parseInt(    prop.getProperty(INGEST_SPEED, "-1"));
        ingestReportFrequency      = Integer.parseInt(    prop.getProperty(INGEST_REPORT_FREQUENCY, "5"));
        ingestDurationStandalone   = Integer.parseInt(    prop.getProperty(INGEST_STANDALONE_DURATION, "-1"));
        ingestDurationEndDate      = LocalDate.parse(     prop.getProperty(INGEST_DURATION_END_DATE, "9999-12-31"));
        ingestTarget               = Target.valueOf(      prop.getProperty(INGEST_TARGET, "influx").toUpperCase().trim());
        ingestTargetRecreate       = Boolean.parseBoolean(prop.getProperty(INGEST_TARGET_RECREATE, "false"));
        ingestTargetSharedInstance = Boolean.parseBoolean(prop.getProperty(INGEST_SHARED_INSTANCE, "false"));
        ingestThreads              = Integer.parseInt(    prop.getProperty(INGEST_THREADS, "1"));

        //Queries
        queriesEnabled           = Boolean.parseBoolean(prop.getProperty(QUERIES_ENABLED, "true"));
        queriesTarget            = Target.valueOf(      prop.getProperty(QUERIES_TARGET, "influx").toUpperCase().trim());
        queriesThreads           = Integer.parseInt(    prop.getProperty(QUERIES_THREADS, "1"));
        queriesSharedInstance    = Boolean.parseBoolean(prop.getProperty(QUERIES_SHARED_INSTANCE, "false"));
        queriesDuration          = Integer.parseInt(    prop.getProperty(QUERIES_DURATION, "60"));
        queriesWarmup            = Integer.parseInt(    prop.getProperty(QUERIES_WARMUP, "5"));
        queriesMaxCount          = Integer.parseInt(    prop.getProperty(QUERIES_MAX_COUNT, "-1"));
        queriesReportFrequency   = Integer.parseInt(    prop.getProperty(QUERIES_REPORT_FREQUENCY, "20"));
        queriesEarliestValidDate = LocalDate.parse(     prop.getProperty(QUERIES_EARLIEST_VALID_DATE, "2019-01-01"));
        queriesWeightTotalClients= Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_TOTAL_CLIENTS, "1"));
        queriesWeightFloorTotals = Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_FLOOR_TOTALS, "1"));
        queriesWeightMaxForAP    = Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_MAX_FOR_AP, "2"));
        queriesWeightAvgOccupancy= Integer.parseInt(    prop.getProperty(QUERIES_WEIGHT_AVG_OCCUPANCY, "1"));
        queriesRngRangeDay       = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_DAY, "0.4"));
        queriesRngRangeWeek      = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_WEEK, "0.7"));
        queriesRngRangeMonth     = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_MONTH, "0.9"));
        queriesRngRangeYear      = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_YEAR, "0.95"));
        queriesIntervalMin       = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MIN, "21600"));
        queriesIntervalMax       = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MAX, "7776000"));
        queriesDateCommIntervalMilliseconds = Integer.parseInt(prop.getProperty(QUERIES_DATE_COMM, "500"));

        //Debug
        debugCreatePrecomputedTables = Boolean.parseBoolean(prop.getProperty(DEBUG_CREATE_PRECOMPUTED_TABLES, "false"));
        debugPrintSettings           = Boolean.parseBoolean(prop.getProperty(DEBUG_PRINT_ALL_SETTINGS, "false"));
    }

    private String validateConfig(){
        // ---- Serialize ----
        if(serialize){
            if(!Paths.get(serializePath).toFile().exists()) return SERIALIZE_PATH + ": Serialize path doesn't exist: " + Paths.get(serializePath).toFile().getAbsolutePath();
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
            if(!(generatorSourceInterval > 0)) return GENERATOR_SOURCE_INTERVAL + ": Source interval must be > 0";
            if(!(generatorGenerationInterval > 0)) return GENERATOR_GENERATION_INTERVAL + ": Generation interval must be > 0";
            if(!(generatorJitter >= 0)) return GENERATOR_JITTER + ": Generation jitter must be >= 0";
            if(!(generatorGenerationInterval == generatorSourceInterval ||
                    (generatorGenerationInterval < generatorSourceInterval && generatorSourceInterval % generatorGenerationInterval == 0) ||
                    (generatorGenerationInterval > generatorSourceInterval && generatorGenerationInterval % generatorSourceInterval == 0))) return GENERATOR_SOURCE_INTERVAL + " and " + GENERATOR_GENERATION_INTERVAL + ": Source interval and generation interval must be equal, or one must be evenly divisible by the other";
        }

        // ---- Ingest ----
        if(ingestEnabled){
            if(!(ingestThreads > 0)) return INGEST_THREADS + ": Ingest threads must be > 0";
            if(ingestTarget == Target.FILE) return "Unsupported ingest target 'FILE' (" + INGEST_TARGET + ")";

            if(generatorEnabled && !ingestTargetRecreate){
                if(!(ingestStartDate.isAfter(generatorStartDate) || ingestStartDate.isEqual(generatorStartDate))) return INGEST_START_DATE + ": Ingest start date " + ingestStartDate + " must be equal/after start date " + generatorStartDate + "(" + GENERATOR_START_DATE + ")";
            }

            if(!queriesEnabled){
                if(!(ingestDurationEndDate.isAfter(ingestStartDate))) return "Ingest end-date (" + INGEST_DURATION_END_DATE + ") must be after start-date (" + INGEST_START_DATE + ") for ingestion to run without also running queries.";
            }
        }

        // ---- Queries ----
        if(queriesEnabled){
            if(queriesTarget == Target.FILE) return "Unsupported query target 'FILE' (" + QUERIES_TARGET + ")";
            if(!(queriesThreads > 0)) return QUERIES_THREADS + ": Query threads must be > 0";
            if(!(queriesDuration > 0 || queriesMaxCount > 0)) return "Query-duration (" + QUERIES_DURATION + ") must be > 0 or max query count (" + QUERIES_MAX_COUNT + ") must be > 0";
            if(!(queriesWeightTotalClients >= 0)) return QUERIES_WEIGHT_TOTAL_CLIENTS + ": Query-weight for 'TotalClients' must be >= 0";
            if(!(queriesWeightFloorTotals >= 0)) return QUERIES_WEIGHT_FLOOR_TOTALS + ": Query-weight for 'FloorTotals' must be >= 0";
            if(!(queriesWeightMaxForAP >= 0)) return QUERIES_WEIGHT_MAX_FOR_AP + ": Query-weight for 'Max For AP' must be >= 0";
            if(!(queriesWeightAvgOccupancy >= 0)) return QUERIES_WEIGHT_AVG_OCCUPANCY + ": Query-weight for 'Avg Occupancy' must be >= 0";
            if(!(queriesIntervalMin >= 0)) return QUERIES_INTERVAL_MIN + ": Minimum query interval must be >= 0";
            if(!(queriesIntervalMax >= 0)) return QUERIES_INTERVAL_MAX + ": Maximum query interval must be >= 0";
            if(!(queriesIntervalMin <= queriesIntervalMax)) return QUERIES_INTERVAL_MIN + " and " + QUERIES_INTERVAL_MAX + ": Minimum query interval must be <= maximum query interval";
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

    public String getSettings(){
        SortedMap<String, Object> settings = new TreeMap<>();
        settings.put(SEED, seed);

        settings.put(SERIALIZE_ENABLED, serialize);
        settings.put(SERIALIZE_PATH, serializePath);

        settings.put(GENERATOR_ENABLED, generatorEnabled);
        settings.put(GENERATOR_IDMAP, generatorIdmap);
        settings.put(GENERATOR_GRANULARITY, generatorGranularity);
        settings.put(GENERATOR_JITTER, generatorJitter);
        settings.put(GENERATOR_SCALE, generatorScale);
        settings.put(GENERATOR_MAP_FOLDER, generatorMapfolder);
        settings.put(GENERATOR_SOURCE_INTERVAL, generatorSourceInterval);
        settings.put(GENERATOR_GENERATION_INTERVAL, generatorGenerationInterval);
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
        settings.put(QUERIES_RNG_RANGE_DAY, queriesRngRangeDay);
        settings.put(QUERIES_RNG_RANGE_WEEK, queriesRngRangeWeek);
        settings.put(QUERIES_RNG_RANGE_MONTH, queriesRngRangeMonth);
        settings.put(QUERIES_RNG_RANGE_YEAR, queriesRngRangeYear);
        settings.put(QUERIES_INTERVAL_MIN, queriesIntervalMin);
        settings.put(QUERIES_INTERVAL_MAX, queriesIntervalMax);
        settings.put(QUERIES_DATE_COMM, queriesDateCommIntervalMilliseconds);

        settings.put(DEBUG_CREATE_PRECOMPUTED_TABLES, debugCreatePrecomputedTables);
        settings.put(DEBUG_PRINT_ALL_SETTINGS, debugPrintSettings);

        StringBuilder sb = new StringBuilder();
        for(String key : settings.keySet()){
            Object val = settings.get(key);

            // Pretty-print the target-array, otherwise we end up with [Benchmark...@garbage]
            if(val instanceof Target[]){
                Target[] value = (Target[]) val;
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

    public int getGeneratorSourceInterval() {
        return generatorSourceInterval;
    }

    public int getGeneratorGenerationInterval() {
        return generatorGenerationInterval;
    }

    public LocalDate getGeneratorStartDate() {
        return generatorStartDate;
    }

    public LocalDate getGeneratorEndDate() {
        return generatorEndDate;
    }

    public Target[] saveGeneratedDataTargets() {
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

    public Target getIngestTarget() {
        return ingestTarget;
    }

    public Target getQueriesTarget() {
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
}
