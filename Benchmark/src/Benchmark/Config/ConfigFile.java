package Benchmark.Config;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Properties;

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
    private static final String SEED = "benchmark.rngseed";
    private int       seed;

    private static final String SERIALIZE_ENABLED = "serialization.enabled";
    private static final String SERIALIZE_PATH    = "serialization.path";
    private boolean   serialize;
    private String    serializePath;

    private static final String GENERATOR_ENABLED                 = "generator.enabled";
    private static final String GENERATOR_SCALE                   = "generator.data.scale";
    private static final String GENERATOR_IDMAP                   = "generator.data.idmap";
    private static final String GENERATOR_MAP_FOLDER              = "generator.data.folder";
    private static final String GENERATOR_SOURCE_INTERVAL         = "generator.data.sourceinterval";
    private static final String GENERATOR_GENERATION_INTERVAL     = "generator.data.generationinterval";
    private static final String GENERATOR_KEEP_FLOOR_ASSOCIATIONS = "generator.data.keepfloorassociations";
    private static final String GENERATOR_START_DATE              = "generator.data.startdate";
    private static final String GENERATOR_END_DATE                = "generator.data.enddate";
    private static final String GENERATOR_CREATE_DEBUG_TABLES     = "generator.data.createdebugtables";
    private static final String GENERATOR_OUTPUT_TARGETS          = "generator.output.targets";
    private static final String GENERATOR_OUTPUT_TO_DISK_TARGET   = "generator.output.filepath";
    private boolean   generatorEnabled;
    private double    generatorScale;
    private String    generatorIdmap;
    private String    generatorMapfolder;
    private int       generatorSourceInterval;
    private int       generatorGenerationInterval;
    private boolean   generatorKeepFloorAssociations;
    private LocalDate generatorStartDate;
    private LocalDate generatorEndDate;
    private boolean   generatorCreateDebugTables;
    private Target[]  generatorOutputTargets;
    private String    generatorToDiskTarget;

    private static final String INFLUX_URL      = "influx.url";
    private static final String INFLUX_USERNAME = "influx.username";
    private static final String INFLUX_PASSWORD = "influx.password";
    private static final String INFLUX_DBNAME   = "influx.dbname";
    private static final String INFLUX_TABLE    = "influx.table";
    private String  influxUrl;
    private String  influxUsername;
    private String  influxPassword;
    private String  influxDBName;
    private String  influxTable;

    private static final String INGEST_ENABLED             = "ingest.enabled";
    private static final String INGEST_START_DATE          = "ingest.startdate";
    private static final String INGEST_SPEED               = "ingest.speed";
    private static final String INGEST_REPORT_FREQUENCY    = "ingest.reportfrequency";
    private static final String INGEST_STANDALONE_DURATION = "ingest.standaloneduration";
    private static final String INGEST_TARGET              = "ingest.target";
    private static final String INGEST_TARGET_RECREATE     = "ingest.target.recreate";
    private static final String INGEST_SHARED_INSTANCE     = "ingest.target.sharedinstance";
    private static final String INGEST_THREADS             = "ingest.threads";
    private boolean   ingestEnabled;
    private LocalDate ingestStartDate;
    private int       ingestSpeed;
    private int       ingestReportFrequency;
    private int       ingestDurationStandalone;
    private Target    ingestTarget;
    private boolean   ingestTargetRecreate;
    private boolean   ingestTargetSharedInstance;
    private int       ingestThreads;

    private static final String QUERIES_ENABLED                  = "queries.enabled";
    private static final String QUERIES_TARGET                   = "queries.target";
    private static final String QUERIES_SHARED_INSTANCE          = "queries.target.sharedinstance";
    private static final String QUERIES_THREADS                  = "queries.threads";
    private static final String QUERIES_DURATION                 = "queries.duration.time";
    private static final String QUERIES_WARMUP                   = "queries.duration.warmup";
    private static final String QUERIES_MAX_COUNT                = "queries.duration.count";
    private static final String QUERIES_REPORT_FREQUENCY         = "queries.reportfrequency";
    private static final String QUERIES_EARLIEST_VALID_DATE      = "queries.earliestdate";
    private static final String QUERIES_PROP_QUERY_TOTAL_CLIENTS = "queries.prop.totalclients";
    private static final String QUERIES_PROP_QUERY_FLOOR_TOTALS  = "queries.prop.floortotals";
    private static final String QUERIES_RNG_LAMBDA               = "queries.lambdaparameter";
    private static final String QUERIES_RNG_RANGE_DAY            = "queries.range.day";
    private static final String QUERIES_RNG_RANGE_WEEK           = "queries.range.week";
    private static final String QUERIES_RNG_RANGE_MONTH          = "queries.range.month";
    private static final String QUERIES_RNG_RANGE_YEAR           = "queries.range.year";
    private static final String QUERIES_INTERVAL_MIN             = "queries.interval.min";
    private static final String QUERIES_INTERVAL_MAX             = "queries.interval.max";
    private boolean   queriesEnabled;
    private Target    queriesTarget;
    private int       queriesThreads;
    private boolean   queriesSharedInstance;
    private int       queriesDuration;
    private int       queriesWarmup;
    private int       queriesMaxCount;
    private int       queriesReportFrequency;
    private LocalDate queriesEarliestValidDate;
    private int       queriesPropTotalClients;
    private int       queriesPropFloorTotals;
    private int       queriesRngLambda;
    private double    queriesRngRangeDay;
    private double    queriesRngRangeWeek;
    private double    queriesRngRangeMonth;
    private double    queriesRngRangeYear;
    private int       queriesIntervalMin;
    private int       queriesIntervalMax;

    private final Properties prop = new Properties();
    private boolean validated;
    private String validationError = "NO ERROR";

    public enum Target{
        INFLUX, FILE
    }

    private ConfigFile(){ }

    public static ConfigFile load(String filePath) throws IOException {
        ConfigFile config = new ConfigFile();
        try(InputStream input = new FileInputStream(filePath)){
            config.prop.load(input);
        }

        config.parseProps();

        String error = config.validateConfig();
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
        config.prop.setProperty(GENERATOR_IDMAP, "FILE PATH");
        config.prop.setProperty(GENERATOR_MAP_FOLDER, "FOLDER PATH");
        config.prop.setProperty(GENERATOR_SOURCE_INTERVAL, "60");
        config.prop.setProperty(GENERATOR_GENERATION_INTERVAL, "60");
        config.prop.setProperty(GENERATOR_KEEP_FLOOR_ASSOCIATIONS, "true");
        config.prop.setProperty(GENERATOR_START_DATE, "2019-01-01");
        config.prop.setProperty(GENERATOR_END_DATE, "2019-04-01");
        config.prop.setProperty(GENERATOR_CREATE_DEBUG_TABLES, "false");
        config.prop.setProperty(GENERATOR_OUTPUT_TARGETS, "influx");
        config.prop.setProperty(GENERATOR_OUTPUT_TO_DISK_TARGET, "TARGET FILE PATH");

        //Influx
        config.prop.setProperty(INFLUX_URL, "http://localhost:8086");
        config.prop.setProperty(INFLUX_USERNAME, "USERNAME");
        config.prop.setProperty(INFLUX_PASSWORD, "PASSWORD");
        config.prop.setProperty(INFLUX_DBNAME, "benchmark");
        config.prop.setProperty(INFLUX_TABLE, "generated");

        //Ingest
        config.prop.setProperty(INGEST_ENABLED, "true");
        config.prop.setProperty(INGEST_START_DATE, "2019-04-01");
        config.prop.setProperty(INGEST_SPEED, "-1");
        config.prop.setProperty(INGEST_REPORT_FREQUENCY, "-1");
        config.prop.setProperty(INGEST_STANDALONE_DURATION, "-1");
        config.prop.setProperty(INGEST_TARGET, "influx");
        config.prop.setProperty(INGEST_TARGET_RECREATE, "false");
        config.prop.setProperty(INGEST_SHARED_INSTANCE, "true");
        config.prop.setProperty(INGEST_THREADS, "1");

        //Queries
        config.prop.setProperty(QUERIES_ENABLED, "true");
        config.prop.setProperty(QUERIES_TARGET, "influx");
        config.prop.setProperty(QUERIES_THREADS, "1");
        config.prop.setProperty(QUERIES_SHARED_INSTANCE, "false");
        config.prop.setProperty(QUERIES_DURATION, "60");
        config.prop.setProperty(QUERIES_WARMUP, "-1");
        config.prop.setProperty(QUERIES_MAX_COUNT, "-1");
        config.prop.setProperty(QUERIES_REPORT_FREQUENCY, "-1");
        config.prop.setProperty(QUERIES_EARLIEST_VALID_DATE, "2019-01-01");
        config.prop.setProperty(QUERIES_PROP_QUERY_TOTAL_CLIENTS, "1");
        config.prop.setProperty(QUERIES_PROP_QUERY_FLOOR_TOTALS, "1");
        config.prop.setProperty(QUERIES_RNG_LAMBDA     , "1");
        config.prop.setProperty(QUERIES_RNG_RANGE_DAY  , "0.5");
        config.prop.setProperty(QUERIES_RNG_RANGE_WEEK , "1");
        config.prop.setProperty(QUERIES_RNG_RANGE_MONTH, "2");
        config.prop.setProperty(QUERIES_RNG_RANGE_YEAR , "3");
        config.prop.setProperty(QUERIES_INTERVAL_MIN   , "21600"); // 6 hours in seconds
        config.prop.setProperty(QUERIES_INTERVAL_MAX   , "7776000"); // 90 days in seconds

        config.parseProps();
        return config;
    }

    @SuppressWarnings("DuplicatedCode")
    private void parseProps(){
        //Benchmark
        seed             = Integer.parseInt(    prop.getProperty(SEED));

        //Serialization
        serialize        = Boolean.parseBoolean(prop.getProperty(SERIALIZE_ENABLED));
        serializePath    =                      prop.getProperty(SERIALIZE_PATH);

        //Generator
        generatorEnabled               = Boolean.parseBoolean(prop.getProperty(GENERATOR_ENABLED));
        generatorIdmap                 =                      prop.getProperty(GENERATOR_IDMAP);
        generatorScale                 = Double.parseDouble(  prop.getProperty(GENERATOR_SCALE));
        generatorMapfolder             =                      prop.getProperty(GENERATOR_MAP_FOLDER);
        generatorSourceInterval        = Integer.parseInt(    prop.getProperty(GENERATOR_SOURCE_INTERVAL));
        generatorGenerationInterval    = Integer.parseInt(    prop.getProperty(GENERATOR_GENERATION_INTERVAL));
        generatorKeepFloorAssociations = Boolean.parseBoolean(prop.getProperty(GENERATOR_KEEP_FLOOR_ASSOCIATIONS));
        generatorStartDate             = LocalDate.parse(     prop.getProperty(GENERATOR_START_DATE));
        generatorEndDate               = LocalDate.parse(     prop.getProperty(GENERATOR_END_DATE));
        generatorCreateDebugTables     = Boolean.parseBoolean(prop.getProperty(GENERATOR_CREATE_DEBUG_TABLES));
        generatorOutputTargets         = Arrays.stream(       prop.getProperty(GENERATOR_OUTPUT_TARGETS).split(","))
                .map(String::toUpperCase).map(String::trim).map(Target::valueOf).toArray(Target[]::new);
        generatorToDiskTarget          =                      prop.getProperty(GENERATOR_OUTPUT_TO_DISK_TARGET);

        //Influx
        influxUrl      = prop.getProperty(INFLUX_URL);
        influxUsername = prop.getProperty(INFLUX_USERNAME);
        influxPassword = prop.getProperty(INFLUX_PASSWORD);
        influxDBName   = prop.getProperty(INFLUX_DBNAME);
        influxTable    = prop.getProperty(INFLUX_TABLE);

        //Ingest
        ingestEnabled              = Boolean.parseBoolean(prop.getProperty(INGEST_ENABLED));
        ingestStartDate            = LocalDate.parse(     prop.getProperty(INGEST_START_DATE));
        ingestSpeed                = Integer.parseInt(    prop.getProperty(INGEST_SPEED));
        ingestReportFrequency      = Integer.parseInt(    prop.getProperty(INGEST_REPORT_FREQUENCY));
        ingestDurationStandalone   = Integer.parseInt(    prop.getProperty(INGEST_STANDALONE_DURATION));
        ingestTarget               = Target.valueOf(      prop.getProperty(INGEST_TARGET).toUpperCase().trim());
        ingestTargetRecreate       = Boolean.parseBoolean(prop.getProperty(INGEST_TARGET_RECREATE));
        ingestTargetSharedInstance = Boolean.parseBoolean(prop.getProperty(INGEST_SHARED_INSTANCE));
        ingestThreads              = Integer.parseInt(    prop.getProperty(INGEST_THREADS));

        //Queries
        queriesEnabled           = Boolean.parseBoolean(prop.getProperty(QUERIES_ENABLED));
        queriesTarget            = Target.valueOf(      prop.getProperty(QUERIES_TARGET).toUpperCase().trim());
        queriesThreads           = Integer.parseInt(    prop.getProperty(QUERIES_THREADS));
        queriesSharedInstance    = Boolean.parseBoolean(prop.getProperty(QUERIES_SHARED_INSTANCE));
        queriesDuration          = Integer.parseInt(    prop.getProperty(QUERIES_DURATION));
        queriesWarmup            = Integer.parseInt(    prop.getProperty(QUERIES_WARMUP));
        queriesMaxCount          = Integer.parseInt(    prop.getProperty(QUERIES_MAX_COUNT));
        queriesReportFrequency   = Integer.parseInt(    prop.getProperty(QUERIES_REPORT_FREQUENCY));
        queriesEarliestValidDate = LocalDate.parse(     prop.getProperty(QUERIES_EARLIEST_VALID_DATE));
        queriesPropTotalClients  = Integer.parseInt(    prop.getProperty(QUERIES_PROP_QUERY_TOTAL_CLIENTS));
        queriesPropFloorTotals   = Integer.parseInt(    prop.getProperty(QUERIES_PROP_QUERY_FLOOR_TOTALS));
        queriesRngLambda         = Integer.parseInt(    prop.getProperty(QUERIES_RNG_LAMBDA));
        queriesRngRangeDay       = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_DAY));
        queriesRngRangeWeek      = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_WEEK));
        queriesRngRangeMonth     = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_MONTH));
        queriesRngRangeYear      = Double.parseDouble(  prop.getProperty(QUERIES_RNG_RANGE_YEAR));
        queriesIntervalMin       = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MIN));
        queriesIntervalMax       = Integer.parseInt(    prop.getProperty(QUERIES_INTERVAL_MAX));
    }

    @SuppressWarnings("ConstantConditions")
    private String validateConfig(){
        // ---- Serialize ----
        if(serialize){
            assert Paths.get(serializePath).toFile().exists();
            if(!Paths.get(serializePath).toFile().exists()) return SERIALIZE_PATH + ": Serialize path doesn't exist: " + Paths.get(serializePath).toFile().getAbsolutePath();
        }
        if(!serialize){
            assert generatorEnabled;
            if(!generatorEnabled) return "Both serialization (" + SERIALIZE_ENABLED + ") and the generator (" + GENERATOR_ENABLED + ") are disabled. One or both must be enabled to create/load the data needed for ingestion and queries.";
        }

        if(serialize || generatorEnabled){
            // Serialization doesn't serialize the source-data that ingestion relies on, so still ensure that those paths are valid.
            assert Paths.get(generatorIdmap).toFile().exists();
            if(!Paths.get(generatorIdmap).toFile().exists()) return GENERATOR_IDMAP + ": Path doesn't exist: " + Paths.get(generatorIdmap).toFile().getAbsolutePath();
            assert Paths.get(generatorMapfolder).toFile().exists();
            if(!Paths.get(generatorMapfolder).toFile().exists()) return GENERATOR_MAP_FOLDER + ": Folder path doesn't exist: " + Paths.get(generatorMapfolder).toFile().getAbsolutePath();
        }

        // ---- Generator ----
        if(generatorEnabled){
            assert generatorScale > 0.0;
            if(!(generatorScale > 0.0)) return GENERATOR_SCALE + ": Scale must be > 0.0";
            assert generatorSourceInterval > 0;
            if(!(generatorSourceInterval > 0)) return GENERATOR_SOURCE_INTERVAL + ": Source interval must be > 0";
            assert generatorGenerationInterval > 0;
            if(!(generatorGenerationInterval > 0)) return GENERATOR_GENERATION_INTERVAL + ": Generation interval must be > 0";
            assert generatorGenerationInterval == generatorSourceInterval || // Intervals match
                    (generatorGenerationInterval < generatorSourceInterval && generatorSourceInterval % generatorGenerationInterval == 0) || // Interval to generate is quicker than source-interval. Then the generate-interval must be evenly divisible by the source-interval
                    (generatorGenerationInterval > generatorSourceInterval && generatorGenerationInterval % generatorSourceInterval == 0) :  // Interval to generate is slower than source-interval.  Then the source-interval must be evenly divisible by the generate-interval
                    "Mismatching intervals. Intervals must match, or one interval must be evenly divisible by the other.\n Generation interval: " + generatorGenerationInterval + ". Source interval: " + generatorSourceInterval;
            if(!(generatorGenerationInterval == generatorSourceInterval ||
                    (generatorGenerationInterval < generatorSourceInterval && generatorSourceInterval % generatorGenerationInterval == 0) ||
                    (generatorGenerationInterval > generatorSourceInterval && generatorGenerationInterval % generatorSourceInterval == 0))) return GENERATOR_SOURCE_INTERVAL + " and " + GENERATOR_GENERATION_INTERVAL + ": Source interval and generation interval must be equal, or one must be evenly divisible by the other";
            assert generatorStartDate.isBefore(generatorEndDate) || generatorStartDate.isEqual(generatorEndDate);
            if(!(generatorStartDate.isBefore(generatorEndDate) || generatorStartDate.isEqual(generatorEndDate))) return GENERATOR_START_DATE + ": Start date " + generatorStartDate + " must be before end date " + generatorEndDate + "(" + GENERATOR_END_DATE + ")";
            assert generatorOutputTargets.length > 0;
            if(!(generatorOutputTargets.length > 0)) return "Generator enabled but no generator targets specified (" + GENERATOR_OUTPUT_TARGETS + ")";
        }

        // ---- Ingest ----
        if(ingestEnabled){
            assert ingestStartDate.isAfter(generatorStartDate) || ingestStartDate.isEqual(generatorStartDate);
            if(!(ingestStartDate.isAfter(generatorStartDate) || ingestStartDate.isEqual(generatorStartDate))) return INGEST_START_DATE + ": Ingest start date " + ingestStartDate + " must be equal/after start date " + generatorStartDate + "(" + GENERATOR_START_DATE + ")";
            assert ingestThreads > 0;
            if(!(ingestThreads > 0)) return INGEST_THREADS + ": Ingest threads must be > 0";
            assert ingestTarget != Target.FILE;
            if(ingestTarget == Target.FILE) return "Unsupported ingest target 'FILE' (" + INGEST_TARGET + ")";

            if(!queriesEnabled){
                assert ingestDurationStandalone > 0;
                if(!(ingestDurationStandalone > 0)) return "Ingestion is enabled but queries aren't. No ingest-duration is set (" + INGEST_STANDALONE_DURATION + ") so ingestion will end immediately.";
            }
        }

        // ---- Queries ----
        if(queriesEnabled){
            assert queriesTarget != Target.FILE;
            if(queriesTarget == Target.FILE) return "Unsupported query target 'FILE' (" + QUERIES_TARGET + ")";
            assert queriesThreads > 0;
            if(!(queriesThreads > 0)) return QUERIES_THREADS + ": Query threads must be > 0";
            assert queriesDuration > 0 || queriesMaxCount > 0;
            if(!(queriesDuration > 0 || queriesMaxCount > 0)) return "Query-duration (" + QUERIES_DURATION + ") must be > 0 or max query count (" + QUERIES_MAX_COUNT + ") must be > 0";
            assert queriesPropTotalClients >= 0;
            if(!(queriesPropTotalClients >= 0)) return QUERIES_PROP_QUERY_TOTAL_CLIENTS + ": Query-probability for 'TotalClients' must be >= 0";
            assert queriesPropFloorTotals >= 0;
            if(!(queriesPropFloorTotals >= 0)) return QUERIES_PROP_QUERY_FLOOR_TOTALS + ": Query-probability for 'FloorTotals' must be >= 0";
            assert queriesIntervalMin >= 0;
            if(!(queriesIntervalMin >= 0)) return QUERIES_INTERVAL_MIN + ": Minimum query interval must be >= 0";
            assert queriesIntervalMax >= 0;
            if(!(queriesIntervalMax >= 0)) return QUERIES_INTERVAL_MAX + ": Maximum query interval must be >= 0";
            assert queriesIntervalMin <= queriesIntervalMax;
            if(!(queriesIntervalMin <= queriesIntervalMax)) return QUERIES_INTERVAL_MIN + " and " + QUERIES_INTERVAL_MAX + ": Minimum query interval must be <= maximum query interval";
        }

        if(ingestEnabled && queriesEnabled){
            assert ingestTarget.equals(queriesTarget);
            if(!(ingestTarget.equals(queriesTarget))) return "Ingestion and queries are both enabled, but have different targets (" + INGEST_TARGET + ", " + QUERIES_TARGET + ")";
        }

        return null;
    }

    public void save(String filePath) throws IOException {
        try(OutputStream output = new FileOutputStream(filePath)){
            prop.store(output, "Config file for Benchmark");
        }
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

    public boolean generatorCreateDebugTables() {
        return generatorCreateDebugTables;
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

    public int getQueriesPropTotalClients() {
        return queriesPropTotalClients;
    }

    public int getQueriesPropFloorTotals() {
        return queriesPropFloorTotals;
    }

    public LocalDate getQueriesEarliestValidDate() {
        return queriesEarliestValidDate;
    }

    public int getQueriesRngLambda() {
        return queriesRngLambda;
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
}
