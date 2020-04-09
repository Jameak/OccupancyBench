package Benchmark;

import Benchmark.Config.ConfigFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Facilitates logging to CSV files. Logged data isn't written to disk until 'writeAllToFile' is called which writes
 * all logging instances to disk. This ensures we do not perform any IO-operations during time-sensitive sections
 * by forcing all log-IO to the very end of the program.
 *
 * TODO: This implementation is quite unwieldy due to the following factors:
 *       1) The desire to keep overhead down to reduce the impact of logging on the timers used in other components in general,
 *          but especially during the tight ingest-generation-control loop where IO would kill performance.
 *       2) The wish to output csv-files that can be copied directly into a spreadsheet without needing any manual changes
 *          to the results. This required all threads working on the same thing (e.g. all query-threads) to have their
 *          output end up in the same file in the end.
 *       3) The wish to have a clear API for each log-type that forces the programmer to pass all the information
 *          that we want to include in the CSV-file, rather than a single 'write(String... contents)' function where no
 *          such thing can be enforced at compile-time.
 *       This should probably be refactored into something more robust / easier to read, but it's good enough for this simple use.
 *       A proper refactor might combine both the STD-OUT-logger and this CSV-logger into a single implementation that
 *       can do both based on the config so that all logging is centralized and static?
 *       Or simply make several 'writeNAME(String thread, String status, ...' etc. methods in the same class to address
 *       the 3rd point? Should be easy to figure out exactly what 'write...' methods are needed now that the initial
 *       implementation has been done.
 */
public abstract class CSVLogger {
    private static final char SEPARATOR = '\t';
    private static final int NUMBER_OF_QUERIES = 5;
    private static final String TOTAL_CLIENTS = "Total Clients";
    private static final String FLOOR_TOTALS  = "Floor Totals";
    private static final String MAX_FOR_AP    = "Max for AP";
    private static final String AVG_OCCUPANCY = "Avg Occupancy";
    private static final String KMEANS        = "K-Means";
    private static final String INGEST_AVERAGE_FILE    = "ingestion_average.csv";
    private static final String INGEST_ENTRIES_FILE    = "ingestion_entries.csv";
    private static final String GENERAL_FILE           = "general.csv";
    private static final String QUERY_SUMMARY_FILE     = "query_summary.csv";
    private static final String QUERY_INDIVIDUALS_FILE = "query_individuals.csv";

    private static final Map<String, CSVLogger> instances = new ConcurrentHashMap<>();
    private boolean done;

    protected final String key;
    protected final PreciseTimer timer;
    protected final LinkedList<Long> timestamps;
    
    private CSVLogger(String key){
        this.key = key;
        this.done = false;
        this.timer = new PreciseTimer();
        this.timestamps = new LinkedList<>();
    }

    private static <T extends CSVLogger> void registerInstance(T instance, String key){
        if(instances.containsKey(key)) throw new IllegalStateException("CSV instance with key " + key + " already exists");
        instances.put(key, instance);
    }

    /**
     * Call to indicate that this logger is done receiving data.
     */
    public void setDone(){
        done = true;
    }

    /**
     * Starts the internal timer used in the logger.
     */
    public void startTimer(){
        timer.start();
    }

    /**
     * Call to write the contents of all CSVLoggers allocated in the program to disk.
     * Log-content is kept in memory until this is called, to remove any IO-overhead during logging.
     */
    public synchronized static void writeAllToDisk(ConfigFile config) throws IOException {
        for(CSVLogger logger : instances.values()){
            assert logger.done : "CSV log-instance " + logger.toString() + " from thread #" + logger.key +
                    " hasn't finished yet.";
            if(!logger.done) Logger.LOG("Warning: CSV log-instance " + logger.toString() + " from thread #" + logger.key +
                    " hasn't finished yet. Data will be written to disk, but some data might be missing.");
        }

        Path csvPath = Paths.get(config.getCSVLogPath());
        Path outPath = csvPath.resolve(config.getCsvFolderPrefix() + config.hashCode() + "");

        boolean madeDir = outPath.toFile().exists() || outPath.toFile().mkdirs();
        if(madeDir){
            Logger.LOG("Writing all csv-logfiles to directory " + outPath.toString());
        } else {
            Logger.LOG("Failed to create directory to write csv-logfiles to: " + outPath.toString());
        }

        List<String> settingsPrint = new ArrayList<>();
        settingsPrint.add(config.toString());
        Files.write(outPath.resolve("config.txt"), settingsPrint, StandardCharsets.UTF_8);

        boolean firstIngestLogger = true;
        boolean firstGeneralLogger = true;
        boolean firstQuerySummaryLogger = true;
        boolean firstIndividualQueryLogger = true;

        outPath.resolve(INGEST_AVERAGE_FILE).toFile().delete();
        outPath.resolve(INGEST_ENTRIES_FILE).toFile().delete();
        outPath.resolve(GENERAL_FILE).toFile().delete();
        outPath.resolve(QUERY_SUMMARY_FILE).toFile().delete();
        outPath.resolve(QUERY_INDIVIDUALS_FILE).toFile().delete();

        for(CSVLogger csv : instances.values()){
            if(csv instanceof IngestLogger){
                IngestLogger ingestLogger = (IngestLogger) csv;
                List<String> averageOutput = new ArrayList<>();
                List<String> entriesOutput = new ArrayList<>();

                if(firstIngestLogger){
                    firstIngestLogger = false;
                    if(config.includeCsvHeaderInOutput()){
                        averageOutput.add(ingestLogger.CSV_AVERAGE_HEADER);
                        entriesOutput.add(ingestLogger.CSV_ENTRIES_HEADER);
                    }
                }

                averageOutput.add(ingestLogger.averageEntriesOverTime());
                entriesOutput.add(ingestLogger.entriesOverTime());
                Files.write(outPath.resolve(INGEST_AVERAGE_FILE), averageOutput, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                Files.write(outPath.resolve(INGEST_ENTRIES_FILE), entriesOutput, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } else if(csv instanceof GeneralLogger){
                GeneralLogger generalLogger = (GeneralLogger) csv;
                List<String> output = new ArrayList<>();

                if(firstGeneralLogger){
                    firstGeneralLogger = false;
                    if(config.includeCsvHeaderInOutput()){
                        output.add(generalLogger.CSV_HEADER);
                    }
                }

                output.add(generalLogger.generalContent());
                Files.write(outPath.resolve(GENERAL_FILE), output, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } else if(csv instanceof QuerySummaryLogger){
                QuerySummaryLogger querySummaryLogger = (QuerySummaryLogger) csv;
                List<String> output = new ArrayList<>();

                if(firstQuerySummaryLogger){
                    firstQuerySummaryLogger = false;
                    if(config.includeCsvHeaderInOutput()){
                        output.add(querySummaryLogger.CSV_HEADER);
                    }
                }

                output.add(querySummaryLogger.summaryOverTime());
                Files.write(outPath.resolve(QUERY_SUMMARY_FILE), output, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } else if(csv instanceof IndividualQueryLogger){
                IndividualQueryLogger individualQueryLogger = (IndividualQueryLogger) csv;
                List<String> output = new ArrayList<>();

                if(firstIndividualQueryLogger){
                    firstIndividualQueryLogger = false;
                    if(config.includeCsvHeaderInOutput()){
                        output.add(individualQueryLogger.CSV_HEADER);
                    }
                }

                output.add(individualQueryLogger.queriesOverTime());
                Files.write(outPath.resolve(QUERY_INDIVIDUALS_FILE), output, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            }
        }
    }

    /**
     * CSVLogger implementation for logging information related to ingestion.
     * Not thread-safe, so each ingestion-thread should get its own instance.
     */
    public static final class IngestLogger extends CSVLogger{
        private final LinkedList<Integer> entries = new LinkedList<>();
        private final LinkedList<Integer> averagedOverReportFrequency = new LinkedList<>();
        public final String CSV_AVERAGE_HEADER = "Time since ingest start (sec)" + SEPARATOR + "Thread" + SEPARATOR + "Entries / sec";
        public final String CSV_ENTRIES_HEADER = "Time since ingest start (sec)" + SEPARATOR + "Thread" + SEPARATOR + "Entries";
        private final int threadNumber;

        private IngestLogger(String key, int threadNumber){
            super(key);
            this.threadNumber = threadNumber;
        }

        public static IngestLogger createInstance(String key, int threadNumber){
            IngestLogger logger = new IngestLogger(key, threadNumber);
            CSVLogger.registerInstance(logger, key);
            return logger;
        }

        public void write(int entriesSinceLastWrite, int entriesPerReportFrequency){
            timestamps.add(timer.elapsedNanoseconds());
            entries.add(entriesSinceLastWrite);
            averagedOverReportFrequency.add(entriesPerReportFrequency);
        }

        public String averageEntriesOverTime(){
            assert timestamps.size() == averagedOverReportFrequency.size();
            Iterator<Long> tsIterator = timestamps.iterator();
            Iterator<Integer> valIterator = averagedOverReportFrequency.iterator();
            StringBuilder sb = new StringBuilder();

            return build(tsIterator, valIterator, sb);
        }

        public String entriesOverTime(){
            assert timestamps.size() == entries.size();
            Iterator<Long> tsIterator = timestamps.iterator();
            Iterator<Integer> valIterator = entries.iterator();
            StringBuilder sb = new StringBuilder();

            return build(tsIterator, valIterator, sb);
        }

        private String build(Iterator<Long> tsIterator, Iterator<Integer> valIterator, StringBuilder sb) {
            boolean first = true;
            while(tsIterator.hasNext() && valIterator.hasNext()){
                long time = tsIterator.next();
                int value = valIterator.next();

                if(first){
                    first = false;
                } else {
                    sb.append("\n");
                }

                sb.append(String.format("%.3f", time / 1e9));
                sb.append(SEPARATOR);
                sb.append(threadNumber);
                sb.append(SEPARATOR);
                sb.append(value);
            }

            return sb.toString();
        }
    }

    /**
     * CSVLogger implementation for logging general information. Thread-safe.
     * Implemented as a single-ton due to the need to keep track of when program execution started.
     */
    public static final class GeneralLogger extends CSVLogger{
        private final LinkedList<String> senders = new LinkedList<>();
        private final LinkedList<String> messages = new LinkedList<>();
        public final String CSV_HEADER = "Time since program start (sec)" + SEPARATOR + "Thread" + SEPARATOR + "Message";

        private static GeneralLogger instance;

        private GeneralLogger(){
            super("GENERAL");
        }

        public static synchronized GeneralLogger createOrGetInstance(){
            if(instance == null){
                instance = new GeneralLogger();
                CSVLogger.registerInstance(instance, "GENERAL");
            }
            return instance;
        }

        public synchronized void write(String sendingThread, String message){
            timestamps.add(timer.elapsedNanoseconds());
            senders.add(sendingThread);
            messages.add(message);
        }

        public synchronized String generalContent(){
            assert timestamps.size() == messages.size() && messages.size() == senders.size();
            Iterator<Long> tsIterator = timestamps.iterator();
            Iterator<String> messageIterator = messages.iterator();
            Iterator<String> sendersIterator = senders.iterator();

            StringBuilder sb = new StringBuilder();

            boolean first = true;
            while(tsIterator.hasNext() && messageIterator.hasNext() && sendersIterator.hasNext()){
                long time = tsIterator.next();
                String message = messageIterator.next();
                String sender = sendersIterator.next();

                if(first){
                    first = false;
                } else {
                    sb.append("\n");
                }

                sb.append(String.format("%.3f", time / 1e9));
                sb.append(SEPARATOR);
                sb.append(sender);
                sb.append(SEPARATOR);
                sb.append(message);
            }

            return sb.toString();
        }
    }

    /**
     * CSVLogger implementation for logging information related to query summary reporting.
     * Not thread-safe, so each ingestion-thread should get its own instance.
     */
    public static final class QuerySummaryLogger extends CSVLogger {
        private final LinkedList<Stats> stats = new LinkedList<>();
        public final String CSV_HEADER = "Time since query start including overhead (sec)" + SEPARATOR + "Thread"
                + SEPARATOR + "Status" + SEPARATOR + "Query name" + SEPARATOR + "Count" + SEPARATOR + "Total time"
                + SEPARATOR + "Queries / sec";
        private final int threadNumber;

        private QuerySummaryLogger(String key, int threadNumber) {
            super(key);
            this.threadNumber = threadNumber;
        }

        public static QuerySummaryLogger createInstance(String key, int threadNumber){
            QuerySummaryLogger logger = new QuerySummaryLogger(key, threadNumber);
            CSVLogger.registerInstance(logger, key);
            return logger;
        }

        public void write(int count_TotalClients, int count_FloorTotal, int count_MaxForAP, int count_AvgOccupancy, int count_KMeans,
                          double totalTime_TotalClients, double totalTime_FloorTotal, double totalTime_MaxForAP, double totalTime_AvgOccupancy, double totalTime_KMeans,
                          double qps_TotalClients, double qps_FloorTotal, double qps_MaxForAP, double qps_AvgOccupancy, double qps_KMeans,
                          boolean done){
            timestamps.add(timer.elapsedNanoseconds());
            stats.add(new Stats(count_TotalClients, count_FloorTotal, count_MaxForAP, count_AvgOccupancy, count_KMeans,
                                totalTime_TotalClients, totalTime_FloorTotal, totalTime_MaxForAP, totalTime_AvgOccupancy, totalTime_KMeans,
                                qps_TotalClients, qps_FloorTotal, qps_MaxForAP, qps_AvgOccupancy, qps_KMeans, done));
        }

        public String summaryOverTime(){
            assert timestamps.size() == stats.size();
            Iterator<Long> tsIterator = timestamps.iterator();
            Iterator<Stats> statsIterator = stats.iterator();

            StringBuilder sb = new StringBuilder();

            boolean first = true;
            while(tsIterator.hasNext() && statsIterator.hasNext()){
                long time = tsIterator.next();
                Stats stat = statsIterator.next();

                if(first){
                    first = false;
                } else {
                    sb.append("\n");
                }

                for(int i = 0; i < NUMBER_OF_QUERIES; i++){
                    sb.append(String.format("%.3f", time / 1e9));
                    sb.append(SEPARATOR);
                    sb.append(threadNumber);
                    sb.append(SEPARATOR);
                    sb.append(stat.done ? "DONE" : "IN-PROGRESS");
                    sb.append(SEPARATOR);
                    if (i == 0) {
                        sb.append(TOTAL_CLIENTS);
                        sb.append(SEPARATOR);
                        sb.append(stat.count_totalClients);
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.totalTime_totalClients));
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.qps_totalClients));
                    } else if (i == 1) {
                        sb.append(FLOOR_TOTALS);
                        sb.append(SEPARATOR);
                        sb.append(stat.count_floorTotal);
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.totalTime_floorTotal));
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.qps_floorTotal));
                    } else if (i == 2){
                        sb.append(MAX_FOR_AP);
                        sb.append(SEPARATOR);
                        sb.append(stat.count_maxForAP);
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.totalTime_maxForAP));
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.qps_maxForAP));
                    } else if (i == 3){
                        sb.append(AVG_OCCUPANCY);
                        sb.append(SEPARATOR);
                        sb.append(stat.count_avgOccupancy);
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.totalTime_avgOccupancy));
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.qps_avgOccupancy));
                    } else if (i == 4){
                        sb.append(KMEANS);
                        sb.append(SEPARATOR);
                        sb.append(stat.count_kMeans);
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.totalTime_kMeans));
                        sb.append(SEPARATOR);
                        sb.append(String.format("%.3f", stat.qps_kMeans));
                    } else {
                        throw new IllegalStateException("Unknown query. Did you add a new one? i = " + i);
                    }

                    // No newline on last loop
                    if(i != NUMBER_OF_QUERIES - 1){
                        sb.append("\n");
                    }
                }

                if(stat.done){
                    sb.append("\n");
                    sb.append(String.format("%.3f", time / 1e9));
                    sb.append(SEPARATOR);
                    sb.append(threadNumber);
                    sb.append(SEPARATOR);
                    sb.append("DONE");
                    sb.append(SEPARATOR);
                    sb.append("TOTAL");
                    sb.append(SEPARATOR);
                    int countFull = stat.count_totalClients + stat.count_floorTotal + stat.count_maxForAP + stat.count_avgOccupancy + stat.count_kMeans;
                    sb.append(countFull);
                    sb.append(SEPARATOR);
                    double totalTime = stat.totalTime_totalClients + stat.totalTime_floorTotal + stat.totalTime_maxForAP + stat.totalTime_avgOccupancy + stat.totalTime_kMeans;
                    sb.append(String.format("%.3f", totalTime));
                    sb.append(SEPARATOR);
                    sb.append(String.format("%.3f", countFull / totalTime));
                }
            }

            return sb.toString();
        }

        private static final class Stats{
            private final int count_totalClients;
            private final int count_floorTotal;
            private final int count_maxForAP;
            private final int count_avgOccupancy;
            private final int count_kMeans;
            private final double totalTime_totalClients;
            private final double totalTime_floorTotal;
            private final double totalTime_maxForAP;
            private final double totalTime_avgOccupancy;
            private final double totalTime_kMeans;
            private final double qps_totalClients;
            private final double qps_floorTotal;
            private final double qps_maxForAP;
            private final double qps_avgOccupancy;
            private final double qps_kMeans;
            private final boolean done;

            public Stats(int count_TotalClients, int count_FloorTotal, int count_MaxForAP, int count_AvgOccupancy, int count_KMeans,
                         double totalTime_TotalClients, double totalTime_FloorTotal, double totalTime_MaxForAP, double totalTime_AvgOccupancy, double totalTime_KMeans,
                         double qps_TotalClients, double qps_FloorTotal, double qps_MaxForAP, double qps_AvgOccupancy, double qps_KMeans,
                         boolean done){
                this.count_totalClients = count_TotalClients;
                this.count_floorTotal = count_FloorTotal;
                this.count_maxForAP = count_MaxForAP;
                this.count_avgOccupancy = count_AvgOccupancy;
                this.count_kMeans = count_KMeans;
                this.totalTime_totalClients = totalTime_TotalClients;
                this.totalTime_floorTotal = totalTime_FloorTotal;
                this.totalTime_maxForAP = totalTime_MaxForAP;
                this.totalTime_avgOccupancy = totalTime_AvgOccupancy;
                this.totalTime_kMeans = totalTime_KMeans;
                this.qps_totalClients = qps_TotalClients;
                this.qps_floorTotal = qps_FloorTotal;
                this.qps_maxForAP = qps_MaxForAP;
                this.qps_avgOccupancy = qps_AvgOccupancy;
                this.qps_kMeans = qps_KMeans;
                this.done = done;
            }
        }
    }

    /**
     * CSVLogger implementation for logging information related to reporting individual query execution times.
     * Not thread-safe, so each ingestion-thread should get its own instance.
     */
    public static final class IndividualQueryLogger extends CSVLogger {
        private final LinkedList<String> queryNames = new LinkedList<>();
        private final LinkedList<Double> queryExecutionTimes = new LinkedList<>();
        public final String CSV_HEADER = "Time since query start including overhead (sec)" +
                SEPARATOR + "Thread" + SEPARATOR + "Query" + SEPARATOR + "Execution time (ms)";
        private final int threadNumber;

        private IndividualQueryLogger(String key, int threadNumber) {
            super(key);
            this.threadNumber = threadNumber;
        }

        public static IndividualQueryLogger createInstance(String key, int threadNumber){
            IndividualQueryLogger logger = new IndividualQueryLogger(key, threadNumber);
            CSVLogger.registerInstance(logger, key);
            return logger;
        }

        public void write(String queryName, double queryExecutionTime){
            timestamps.add(timer.elapsedNanoseconds());
            queryNames.add(queryName);
            queryExecutionTimes.add(queryExecutionTime);
        }

        public String queriesOverTime(){
            assert timestamps.size() == queryNames.size() && queryNames.size() == queryExecutionTimes.size();
            Iterator<Long> tsIterator = timestamps.iterator();
            Iterator<String> nameIterator = queryNames.iterator();
            Iterator<Double> executionIterator = queryExecutionTimes.iterator();
            StringBuilder sb = new StringBuilder();

            boolean first = true;
            while(tsIterator.hasNext() && nameIterator.hasNext() && executionIterator.hasNext()){
                long time = tsIterator.next();
                String name = nameIterator.next();
                double executionTime = executionIterator.next();

                if(first){
                    first = false;
                } else {
                    sb.append("\n");
                }

                sb.append(String.format("%.3f", time / 1e9));
                sb.append(SEPARATOR);
                sb.append(threadNumber);
                sb.append(SEPARATOR);
                sb.append(name);
                sb.append(SEPARATOR);
                sb.append(String.format("%.3f", executionTime));
            }

            return sb.toString();
        }
    }
}
