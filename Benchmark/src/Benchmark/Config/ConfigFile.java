package Benchmark.Config;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Properties;

public class ConfigFile {
    private static final String GENERATE_INITIAL_DATA = "benchmark.generateinitialdata";
    private static final String RUN_QUERIES           = "benchmark.runqueries";
    private static final String INGEST                = "benchmark.ingest";
    private static final String SCALE                 = "benchmark.scale";
    private static final String SEED                  = "benchmark.rngseed";
    private static final String SERIALIZE             = "benchmark.serialize";
    private static final String SERIALIZE_PATH        = "benchmark.serializepath";
    private static final String THREADS_INGEST        = "benchmark.threads.ingest";
    private static final String THREADS_QUERIES       = "benchmark.threads.queries";
    private boolean   generatedata;
    private boolean   runqueries;
    private boolean   ingest;
    private double    scale;
    private int       seed;
    private boolean   serialize;
    private String    serializePath;
    private int       threadsIngest;
    private int       threadsQueries;

    private static final String IDMAP                   = "generator.data.idmap";
    private static final String MAP_FOLDER              = "generator.data.folder";
    private static final String SOURCE_INTERVAL         = "generator.data.sourceinterval";
    private static final String GENERATION_INTERVAL     = "generator.data.generationinterval";
    private static final String KEEP_FLOOR_ASSOCIATIONS = "generator.data.keepfloorassociations";
    private static final String START_DATE              = "generator.data.startdate";
    private static final String END_DATE                = "generator.data.enddate";
    private static final String TO_DISK                 = "generator.data.todisk";
    private static final String TO_DISK_TARGET          = "generator.data.todisk.target";
    private static final String TO_INFLUX               = "generator.data.toinflux";
    private static final String CREATE_DEBUG_TABLES     = "generator.data.createdebugtables";
    private String    idmap;
    private String    mapfolder;
    private int       sourceinterval;
    private int       generationinterval;
    private boolean   keepFloorAssociations;
    private LocalDate startDate;
    private LocalDate endDate;
    private boolean   toDisk;
    private String    toDiskTarget;
    private boolean   toInflux;
    private boolean   createDebugTables;

    private static final String INFLUX_URL            = "output.influx.url";
    private static final String INFLUX_USERNAME       = "output.influx.username";
    private static final String INFLUX_PASSWORD       = "output.influx.password";
    private static final String INFLUX_DBNAME         = "output.influx.dbname";
    private static final String INFLUX_TABLE          = "output.influx.table";
    private String    influxUrl;
    private String    influxUsername;
    private String    influxPassword;
    private String    influxDBName;
    private String    influxTable;

    private static final String INGEST_START_DATE          = "ingest.startdate";
    private static final String INGEST_DESIRED_SPEED       = "ingest.desiredspeed";
    private static final String INGEST_REPORT_FREQUENCY    = "ingest.reportfrequency";
    private static final String INGEST_STANDALONE_DURATION = "ingest.standaloneduration";
    private LocalDate ingestStartDate;
    private int       desiredSpeed;
    private int       reportFrequency;
    private int       durationStandalone;

    private static final String QUERIES_DURATION                 = "queries.duration.time";
    private static final String QUERIES_WARMUP                   = "queries.duration.warmup";
    private static final String QUERIES_MAX_COUNT                = "queries.duration.count";
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
    private int       queriesDuration;
    private int       queriesWarmup;
    private int       queriesMaxCount;
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

        config.prop.setProperty(GENERATE_INITIAL_DATA, "true");
        config.prop.setProperty(RUN_QUERIES, "false");
        config.prop.setProperty(INGEST, "false");
        config.prop.setProperty(SCALE, "1.0");
        config.prop.setProperty(SEED, "1234");
        config.prop.setProperty(SERIALIZE, "false");
        config.prop.setProperty(SERIALIZE_PATH, "FOLDER PATH");
        config.prop.setProperty(THREADS_INGEST, "1");
        config.prop.setProperty(THREADS_QUERIES, "1");

        config.prop.setProperty(IDMAP, "FILE PATH");
        config.prop.setProperty(MAP_FOLDER, "FOLDER PATH");
        config.prop.setProperty(SOURCE_INTERVAL, "60");
        config.prop.setProperty(GENERATION_INTERVAL, "60");
        config.prop.setProperty(KEEP_FLOOR_ASSOCIATIONS, "true");
        config.prop.setProperty(START_DATE, "2019-01-01");
        config.prop.setProperty(END_DATE, "2019-03-31");
        config.prop.setProperty(TO_DISK, "true");
        config.prop.setProperty(TO_DISK_TARGET, "TARGET FILE PATH");
        config.prop.setProperty(TO_INFLUX, "false");
        config.prop.setProperty(CREATE_DEBUG_TABLES, "false");

        config.prop.setProperty(INFLUX_URL, "http://localhost:8086");
        config.prop.setProperty(INFLUX_USERNAME, "USERNAME");
        config.prop.setProperty(INFLUX_PASSWORD, "PASSWORD");
        config.prop.setProperty(INFLUX_DBNAME, "benchmark");
        config.prop.setProperty(INFLUX_TABLE, "generated");

        config.prop.setProperty(INGEST_START_DATE, "2019-04-01");
        config.prop.setProperty(INGEST_DESIRED_SPEED, "-1");
        config.prop.setProperty(INGEST_REPORT_FREQUENCY, "-1");
        config.prop.setProperty(INGEST_STANDALONE_DURATION, "-1");

        config.prop.setProperty(QUERIES_DURATION, "60");
        config.prop.setProperty(QUERIES_WARMUP, "-1");
        config.prop.setProperty(QUERIES_MAX_COUNT, "-1");
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

    private void parseProps(){
        generatedata     = Boolean.parseBoolean(prop.getProperty(GENERATE_INITIAL_DATA));
        runqueries       = Boolean.parseBoolean(prop.getProperty(RUN_QUERIES));
        ingest           = Boolean.parseBoolean(prop.getProperty(INGEST));
        scale            = Double.parseDouble(  prop.getProperty(SCALE));
        seed             = Integer.parseInt(    prop.getProperty(SEED));
        serialize        = Boolean.parseBoolean(prop.getProperty(SERIALIZE));
        serializePath    =                      prop.getProperty(SERIALIZE_PATH);
        threadsIngest    = Integer.parseInt(    prop.getProperty(THREADS_INGEST));
        threadsQueries   = Integer.parseInt(    prop.getProperty(THREADS_QUERIES));

        idmap                 =                      prop.getProperty(IDMAP);
        mapfolder             =                      prop.getProperty(MAP_FOLDER);
        sourceinterval        = Integer.parseInt(    prop.getProperty(SOURCE_INTERVAL));
        generationinterval    = Integer.parseInt(    prop.getProperty(GENERATION_INTERVAL));
        keepFloorAssociations = Boolean.parseBoolean(prop.getProperty(KEEP_FLOOR_ASSOCIATIONS));
        startDate             = LocalDate.parse(     prop.getProperty(START_DATE));
        endDate               = LocalDate.parse(     prop.getProperty(END_DATE));
        toDisk                = Boolean.parseBoolean(prop.getProperty(TO_DISK));
        toDiskTarget          =                      prop.getProperty(TO_DISK_TARGET);
        toInflux              = Boolean.parseBoolean(prop.getProperty(TO_INFLUX));
        createDebugTables     = Boolean.parseBoolean(prop.getProperty(CREATE_DEBUG_TABLES));

        influxUrl      = prop.getProperty(INFLUX_URL);
        influxUsername = prop.getProperty(INFLUX_USERNAME);
        influxPassword = prop.getProperty(INFLUX_PASSWORD);
        influxDBName   = prop.getProperty(INFLUX_DBNAME);
        influxTable    = prop.getProperty(INFLUX_TABLE);

        ingestStartDate    = LocalDate.parse( prop.getProperty(INGEST_START_DATE));
        desiredSpeed       = Integer.parseInt(prop.getProperty(INGEST_DESIRED_SPEED));
        reportFrequency    = Integer.parseInt(prop.getProperty(INGEST_REPORT_FREQUENCY));
        durationStandalone = Integer.parseInt(prop.getProperty(INGEST_STANDALONE_DURATION));

        queriesDuration          = Integer.parseInt(  prop.getProperty(QUERIES_DURATION));
        queriesWarmup            = Integer.parseInt(  prop.getProperty(QUERIES_WARMUP));
        queriesMaxCount          = Integer.parseInt(  prop.getProperty(QUERIES_MAX_COUNT));
        queriesEarliestValidDate = LocalDate.parse(   prop.getProperty(QUERIES_EARLIEST_VALID_DATE));
        queriesPropTotalClients  = Integer.parseInt(  prop.getProperty(QUERIES_PROP_QUERY_TOTAL_CLIENTS));
        queriesPropFloorTotals   = Integer.parseInt(  prop.getProperty(QUERIES_PROP_QUERY_FLOOR_TOTALS));
        queriesRngLambda         = Integer.parseInt(  prop.getProperty(QUERIES_RNG_LAMBDA));
        queriesRngRangeDay       = Double.parseDouble(prop.getProperty(QUERIES_RNG_RANGE_DAY));
        queriesRngRangeWeek      = Double.parseDouble(prop.getProperty(QUERIES_RNG_RANGE_WEEK));
        queriesRngRangeMonth     = Double.parseDouble(prop.getProperty(QUERIES_RNG_RANGE_MONTH));
        queriesRngRangeYear      = Double.parseDouble(prop.getProperty(QUERIES_RNG_RANGE_YEAR));
        queriesIntervalMin       = Integer.parseInt(  prop.getProperty(QUERIES_INTERVAL_MIN));
        queriesIntervalMax       = Integer.parseInt(  prop.getProperty(QUERIES_INTERVAL_MAX));
    }

    @SuppressWarnings("ConstantConditions")
    private String validateConfig(){
        // ---- Benchmark ----
        assert scale > 0.0;
        if(!(scale > 0.0)) return SCALE + ": Scale must be > 0.0";
        assert threadsIngest >= 0;
        if(!(threadsIngest >= 0)) return THREADS_INGEST + ": Ingest threads must be >= 0";
        assert threadsQueries >= 0;
        if(!(threadsQueries >= 0)) return THREADS_QUERIES + ": Query threads must be >= 0";
        if(serialize){
            assert Paths.get(serializePath).toFile().exists();
            if(!Paths.get(serializePath).toFile().exists()) return SERIALIZE_PATH + ": Serialize path doesn't exist: " + Paths.get(serializePath).toFile().getAbsolutePath();
        }

        // ---- Generator ----
        assert Paths.get(idmap).toFile().exists();
        if(!Paths.get(idmap).toFile().exists()) return IDMAP + ": Path doesn't exist: " + Paths.get(idmap).toFile().getAbsolutePath();
        assert Paths.get(mapfolder).toFile().exists();
        if(!Paths.get(mapfolder).toFile().exists()) return MAP_FOLDER + ": Folder path doesn't exist: " + Paths.get(mapfolder).toFile().getAbsolutePath();
        assert sourceinterval > 0;
        if(!(sourceinterval > 0)) return SOURCE_INTERVAL + ": Source interval must be > 0";
        assert generationinterval > 0;
        if(!(generationinterval > 0)) return GENERATION_INTERVAL + ": Generation interval must be > 0";
        assert generationinterval == sourceinterval || // Intervals match
                (generationinterval < sourceinterval && sourceinterval % generationinterval == 0) || // Interval to generate is quicker than source-interval. Then the generate-interval must be evenly divisible by the source-interval
                (generationinterval > sourceinterval && generationinterval % sourceinterval == 0) :  // Interval to generate is slower than source-interval.  Then the source-interval must be evenly divisible by the generate-interval
                "Mismatching intervals. Intervals must match, or one interval must be evenly divisible by the other.\n Generation interval: " + generationinterval + ". Source interval: " + sourceinterval;
        if(!(generationinterval == sourceinterval ||
                (generationinterval < sourceinterval && sourceinterval % generationinterval == 0) ||
                (generationinterval > sourceinterval && generationinterval % sourceinterval == 0))) return SOURCE_INTERVAL + " and " + GENERATION_INTERVAL + ": Source interval and generation interval must be equal, or one must be evenly divisible by the other";
        assert startDate.isBefore(endDate) || startDate.isEqual(endDate);
        if(!(startDate.isBefore(endDate) || startDate.isEqual(endDate))) return START_DATE + ": Start date " + startDate + " must be before end date " + endDate + "(" + END_DATE + ")";
        // Cannot assert that the TO_DISK_TARGET exists because we're writing to that path, creating it if it doesn't exist.
        //if(toDisk){
        //    assert Paths.get(toDiskTarget).toFile().exists();
        //    if(!Paths.get(toDiskTarget).toFile().exists()) return TO_DISK_TARGET + ": To Disk target path doesn't exist: " + Paths.get(toDiskTarget).toFile().getAbsolutePath();
        //}

        // ---- Ingest ----
        assert ingestStartDate.isAfter(startDate) || ingestStartDate.isEqual(startDate);
        if(!(ingestStartDate.isAfter(startDate) || ingestStartDate.isEqual(startDate))) return INGEST_START_DATE + ": Ingest start date " + ingestStartDate + " must be equal/after start date " + startDate + "(" + START_DATE + ")";

        // ---- Queries ----
        if(ingest) {
            assert queriesDuration > 0 || queriesMaxCount > 0;
            if(!(queriesDuration > 0 || queriesMaxCount > 0)) return "Query-duration (" + QUERIES_DURATION + ") must be > 0 or max query count (" + QUERIES_MAX_COUNT + ") must be > 0";
        }
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

    public boolean generatedata() {
        return generatedata;
    }

    public boolean runqueries() {
        return runqueries;
    }

    public boolean ingest() {
        return ingest;
    }

    public double scale() {
        return scale;
    }

    public int seed() {
        return seed;
    }

    public String idmap() {
        return idmap;
    }

    public String mapfolder() {
        return mapfolder;
    }

    public int sourceinterval() {
        return sourceinterval;
    }

    public int generationinterval() {
        return generationinterval;
    }

    public LocalDate startDate() {
        return startDate;
    }

    public LocalDate endDate() {
        return endDate;
    }

    public boolean saveToDisk() {
        return toDisk;
    }

    public boolean saveToInflux() {
        return toInflux;
    }

    public String influxUrl() {
        return influxUrl;
    }

    public String influxUsername() {
        return influxUsername;
    }

    public String influxPassword() {
        return influxPassword;
    }

    public String influxDBName() {
        return influxDBName;
    }

    public String influxTable() {
        return influxTable;
    }

    public String toDiskTarget() {
        return toDiskTarget;
    }

    public boolean keepFloorAssociations() {
        return keepFloorAssociations;
    }

    public boolean createDebugTables() {
        return createDebugTables;
    }

    public boolean serialize() {
        return serialize;
    }

    public String serializePath() {
        return serializePath;
    }

    public LocalDate ingestStartDate() {
        return ingestStartDate;
    }

    public int desiredIngestSpeed() {
        return desiredSpeed;
    }

    public int reportFrequency() {
        return reportFrequency;
    }

    public int queriesDuration() {
        return queriesDuration;
    }

    public int queriesWarmup() {
        return queriesWarmup;
    }

    public int queriesPropTotalClients() {
        return queriesPropTotalClients;
    }

    public int queriesPropFloorTotals() {
        return queriesPropFloorTotals;
    }

    public LocalDate queriesEarliestValidDate() {
        return queriesEarliestValidDate;
    }

    public int queriesRngLambda() {
        return queriesRngLambda;
    }

    public double queriesRngRangeDay() {
        return queriesRngRangeDay;
    }

    public double queriesRngRangeWeek() {
        return queriesRngRangeWeek;
    }

    public double queriesRngRangeMonth() {
        return queriesRngRangeMonth;
    }

    public double queriesRngRangeYear() {
        return queriesRngRangeYear;
    }

    public int queriesIntervalMin() {
        return queriesIntervalMin;
    }

    public int queriesIntervalMax() {
        return queriesIntervalMax;
    }

    public int threadsIngest() {
        return threadsIngest;
    }

    public int threadsQueries() {
        return threadsQueries;
    }

    public int durationStandalone() {
        return durationStandalone;
    }

    public int queriesMaxCount() {
        return queriesMaxCount;
    }
}
