package Benchmark.Config;

import java.io.*;
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
    private boolean   generatedata;
    private boolean   runqueries;
    private boolean   ingest;
    private double    scale;
    private int       seed;
    private boolean   serialize;
    private String    serializePath;

    private static final String IDMAP                   = "generator.data.idmap";
    private static final String MAP_FOLDER              = "generator.data.folder";
    private static final String ENTRY_INTERVAL          = "generator.data.entryinterval";
    private static final String GENERATION_INTERVAL     = "generator.data.generationinterval";
    private static final String KEEP_FLOOR_ASSOCIATIONS = "generator.data.keepfloorassociations";
    private static final String START_DATE              = "generator.data.startdate";
    private static final String END_DATE                = "generator.data.enddate";
    private static final String TO_DISK                 = "generator.data.todisk";
    private static final String TO_DISK_FOLDER          = "generator.data.todisk.folder";
    private static final String TO_DISK_FILENAME        = "generator.data.todisk.filename";
    private static final String TO_INFLUX               = "generator.data.toinflux";
    private static final String CREATE_DEBUG_TABLES     = "generator.data.createdebugtables";
    private String    idmap;
    private String    mapfolder;
    private int       entryinterval;
    private int       generationinterval;
    private boolean   keepFloorAssociations;
    private LocalDate startDate;
    private LocalDate endDate;
    private boolean   toDisk;
    private String    toDiskFolder;
    private String    toDiskFilename;
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

    private final Properties prop = new Properties();

    private ConfigFile(){ }

    public static ConfigFile load(String filePath) throws IOException {
        ConfigFile config = new ConfigFile();
        try(InputStream input = new FileInputStream(filePath)){
            config.prop.load(input);
        }

        config.parseProps();
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
        config.prop.setProperty(SERIALIZE_PATH, "FILE PATH");

        config.prop.setProperty(IDMAP, "FILE PATH");
        config.prop.setProperty(MAP_FOLDER, "FOLDER PATH");
        config.prop.setProperty(ENTRY_INTERVAL, "60");
        config.prop.setProperty(GENERATION_INTERVAL, "60");
        config.prop.setProperty(KEEP_FLOOR_ASSOCIATIONS, "true");
        config.prop.setProperty(START_DATE, "2019-01-01");
        config.prop.setProperty(END_DATE, "2019-03-31");
        config.prop.setProperty(TO_DISK, "true");
        config.prop.setProperty(TO_DISK_FOLDER, "FOLDER PATH");
        config.prop.setProperty(TO_DISK_FILENAME, "FILENAME");
        config.prop.setProperty(TO_INFLUX, "false");
        config.prop.setProperty(CREATE_DEBUG_TABLES, "false");

        config.prop.setProperty(INFLUX_URL, "http://localhost:8086");
        config.prop.setProperty(INFLUX_USERNAME, "USERNAME");
        config.prop.setProperty(INFLUX_PASSWORD, "PASSWORD");
        config.prop.setProperty(INFLUX_DBNAME, "benchmark");
        config.prop.setProperty(INFLUX_TABLE, "generated");

        config.parseProps();
        return config;
    }

    private void parseProps(){
        generatedata  = Boolean.parseBoolean(prop.getProperty(GENERATE_INITIAL_DATA));
        runqueries    = Boolean.parseBoolean(prop.getProperty(RUN_QUERIES));
        ingest        = Boolean.parseBoolean(prop.getProperty(INGEST));
        scale         = Double.parseDouble(  prop.getProperty(SCALE));
        seed          = Integer.parseInt(    prop.getProperty(SEED));
        serialize     = Boolean.parseBoolean(prop.getProperty(SERIALIZE));
        serializePath =                      prop.getProperty(SERIALIZE_PATH);

        idmap                 =                      prop.getProperty(IDMAP);
        mapfolder             =                      prop.getProperty(MAP_FOLDER);
        entryinterval         = Integer.parseInt(    prop.getProperty(ENTRY_INTERVAL));
        generationinterval    = Integer.parseInt(    prop.getProperty(GENERATION_INTERVAL));
        keepFloorAssociations = Boolean.parseBoolean(prop.getProperty(KEEP_FLOOR_ASSOCIATIONS));
        startDate             = LocalDate.parse(     prop.getProperty(START_DATE));
        endDate               = LocalDate.parse(     prop.getProperty(END_DATE));
        toDisk                = Boolean.parseBoolean(prop.getProperty(TO_DISK));
        toDiskFolder          =                      prop.getProperty(TO_DISK_FOLDER);
        toDiskFilename        =                      prop.getProperty(TO_DISK_FILENAME);
        toInflux              = Boolean.parseBoolean(prop.getProperty(TO_INFLUX));
        createDebugTables     = Boolean.parseBoolean(prop.getProperty(CREATE_DEBUG_TABLES));

        influxUrl      = prop.getProperty(INFLUX_URL);
        influxUsername = prop.getProperty(INFLUX_USERNAME);
        influxPassword = prop.getProperty(INFLUX_PASSWORD);
        influxDBName   = prop.getProperty(INFLUX_DBNAME);
        influxTable    = prop.getProperty(INFLUX_TABLE);
    }

    public void save(String filePath) throws IOException {
        try(OutputStream output = new FileOutputStream(filePath)){
            prop.store(output, "Config file for Benchmark");
        }
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

    public int entryinterval() {
        return entryinterval;
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

    public String toDiskFolder() {
        return toDiskFolder;
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

    public String toDiskFilename() {
        return toDiskFilename;
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
}
