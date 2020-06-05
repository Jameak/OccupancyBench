import Benchmark.*;
import Benchmark.Databases.SchemaFormats;
import Benchmark.Debug.*;
import Benchmark.Config.ConfigFile;
import Benchmark.Databases.DBTargets;
import Benchmark.Databases.DatabaseTargetFactory;
import Benchmark.Databases.DatabaseQueriesFactory;
import Benchmark.Generator.*;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.Generator.Ingest.IngestRunnable;
import Benchmark.SeedLoader.LoaderFacade;
import Benchmark.Generator.Targets.*;
import Benchmark.SeedLoader.SeedData;
import Benchmark.SeedLoader.Seeddata.SeedEntries;
import Benchmark.Queries.IQueries;
import Benchmark.Queries.QueryRunnable;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The entry-point of the benchmark program.
 */
public class Entrypoint {
    public static void main(String[] args) {
        if(args == null || args.length < 1){
            System.out.println("No arguments provided.");
            System.out.println("Either provide path to a config file or pass --default-config to generate a default config-file in the current working directory.");
            return;
        }

        if(args[0].equalsIgnoreCase("--default-config")){
            Path cwd = FileSystems.getDefault().getPath(".");
            ConfigFile defaultConfig = ConfigFile.defaultConfig();
            String savePath = cwd.resolve("default.config").toString();
            try {
                defaultConfig.save(savePath);
                System.out.println("File default.config created at path: " + savePath);
            } catch (IOException e) {
                System.out.println("Cannot save config to path: " + savePath + "\n");
                e.printStackTrace();
            }
            return;
        }

        ConfigFile config;
        try {
            config = ConfigFile.load(args);
        } catch (IOException e) {
            System.out.println("Cannot load config from path or paths:");
            for(String arg : args){
                System.out.println("    " + arg);
            }
            e.printStackTrace();
            return;
        }

        Set<String> unknownKeys = config.getUnknownKeysInInput();
        if(!unknownKeys.isEmpty()){
            System.out.println("Warning: The following keys in the given config file were not recognized. Did you misspell the key?");
            for(String key : unknownKeys){
                System.out.println("    " + key);
            }
        }

        if(!config.isValidConfig()){
            System.out.println("Config has invalid values: " + config.getValidationError());
            System.out.println("Aborting.");
            return;
        }

        if(config.DEBUG_printSettings()){
            System.out.println(config.toString());
        }

        try {
            new Entrypoint().run(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- Ingestion ---
    private ExecutorService threadPoolIngest;
    private Future[] ingestTasks;
    private IngestRunnable[] ingestRunnables;
    private ITarget[] ingestTargets;

    // --- Querying ---
    private ExecutorService threadPoolQueries;

    private PartitionLockstepChannel DEBUG_partitionLockstepChannel;

    public void run(ConfigFile config) throws Exception{
        assert config.isValidConfig();
        Random rng = new Random(config.getSeed());
        DateCommunication dateComm = new DateCommunication();
        if(config.doLoggingToCSV()){
            CSVLogger.GeneralLogger logger = CSVLogger.GeneralLogger.createOrGetInstance();
            logger.startTimer();
        }

        SeedData parsedData = null;
        if (config.isGeneratorEnabled() || config.isIngestionEnabled()) {
            parsedData = parseMapData(config);
        }

        GeneratedFloor[] generatedFloors;
        if(config.isGeneratorEnabled()){
            generatedFloors = generateFloorData(config, rng, parsedData);
        } else {
            assert config.doSerialization();
            // Load the data from previous run
            Logger.LOG("Deserializing floor.");
            generatedFloors = deserializeFloor(config);
            Logger.LOG(String.format("Deserialized metadata for %s floors and %s APs", generatedFloors.length, GeneratedFloor.allAPsOnFloors(generatedFloors).length));
            Logger.LOG("Deserializing rng.");
            rng = deserializeRandom(config);
        }

        dateComm.setInitialDate(config.getGeneratorEndDate(), LocalTime.of(0,0,0));

        // Create separate rng-sources for ingestion- and querying so that e.g. changing the number of ingest-threads
        //   between runs of the benchmark doesn't change the rng-source for queries (and therefore keeps the
        //   query-workload as comparable as possible)
        Random ingestRngSource = new Random(rng.nextInt());
        Random queryRngSource = new Random(rng.nextInt());

        if(config.DEBUG_isPartitionLockstepEnabled()){
            DEBUG_partitionLockstepChannel = new PartitionLockstepChannel(config);
        }

        if(config.isIngestionEnabled()){
            Logger.LOG("Starting ingestion.");
            startIngestion(config, generatedFloors, parsedData.seedEntries, dateComm, ingestRngSource, !config.doDateCommunicationByQueryingDatabase());
            Logger.LOG("Ingestion started.");
        }

        Future[] queryTasks = null;
        if(config.isQueryingEnabled()){
            Logger.LOG("Starting queries.");
            queryTasks = startQueries(config, dateComm, queryRngSource, generatedFloors);
            Logger.LOG("Queries started.");
        }

        if(config.isQueryingEnabled()){
            // First, wait for queries to finish, since they have a specified duration
            assert queryTasks != null;
            for(Future queryTask : queryTasks){
                queryTask.get();
            }
        } else if(config.isIngestionEnabled()) {
            // If we aren't running the queries, but are running ingestion, then we need some other way to determine
            //   how long to run ingestion for. Otherwise they'll just stop immediately.
            boolean timeDuration = config.getIngestionStandaloneDuration() > 0;

            CoarseTimer timer = new CoarseTimer();
            timer.start();
            while(!timeDuration || timer.elapsedSeconds() < config.getIngestionStandaloneDuration()){
                boolean allDone = true;
                for (IngestRunnable ingestRunnable : ingestRunnables) {
                    if (!ingestRunnable.isDone()) {
                        allDone = false;
                        break;
                    }
                }
                if(allDone){
                    Logger.LOG("All ingest-threads reached configured end-date (" + config.getIngestEndDate().toString() + "). Ending ingestion.");
                    break;
                }

                // The run-time can be limited by:
                // - the duration in seconds. This is specified in seconds, so sleeping for 1 second wont make us miss it by a lot.
                // - the end-date of ingestion. When the end-date is reached ingestion automatically stops, so sleeping wont make us miss the stop-date at all.
                Thread.sleep(1000);
            }

            if(timeDuration && timer.elapsedSeconds() >= config.getIngestionStandaloneDuration()){
                Logger.LOG("Ingestion duration reached. Ending ingestion.");
            }
        }

        if(config.DEBUG_isPartitionLockstepEnabled()){
            DEBUG_partitionLockstepChannel.breakBarriers();
        }

        if(config.isIngestionEnabled()){
            stopIngestion();
        }

        if(config.isQueryingEnabled()) threadPoolQueries.shutdown();

        if(config.doLoggingToCSV()){
            CSVLogger.GeneralLogger.createOrGetInstance().setDone();
            Logger.LOG("Writing to CSV files.");
            CSVLogger.writeAllToDisk(config);
        }

        Logger.LOG("Done.");
    }

    private Future[] startQueries(ConfigFile config, DateCommunication dateComm, Random queryRngSource, GeneratedFloor[] generatedFloors) {
        assert config.getQueriesThreadCount() > 0;
        threadPoolQueries = Executors.newFixedThreadPool(config.getQueriesThreadCount());

        IQueries queryInstance = null;
        if(config.useSharedQueriesInstance()) queryInstance = instantiateQueries(config);

        Future[] queryTasks = new Future[config.getQueriesThreadCount()];
        for(int i = 0; i < config.getQueriesThreadCount(); i++){
            Random queryRngForThisThread = new Random(queryRngSource.nextInt());
            if(!config.useSharedQueriesInstance()) queryInstance = instantiateQueries(config);

            QueryRunnable queryRunnable = new QueryRunnable(config, queryRngForThisThread, dateComm, generatedFloors, queryInstance, "Query " + i, i);
            queryTasks[i] = threadPoolQueries.submit(queryRunnable);
        }
        return queryTasks;
    }

    private void startIngestion(ConfigFile config, GeneratedFloor[] generatedFloors, SeedEntries seedEntries, DateCommunication dateComm, Random ingestRngSource, boolean doDirectComm) throws IOException, SQLException {
        assert config.getIngestThreadCount() > 0;
        threadPoolIngest = Executors.newFixedThreadPool(config.getIngestThreadCount());
        ingestTasks = new Future[config.getIngestThreadCount()];
        ingestRunnables = new IngestRunnable[config.getIngestThreadCount()];
        GeneratedAccessPoint[] allAPs = GeneratedFloor.allAPsOnFloors(generatedFloors);

        ingestTargets = new ITarget[config.useSharedIngestInstance() ? 1 : config.getIngestThreadCount()];
        if(config.useSharedIngestInstance()) ingestTargets[0] = DatabaseTargetFactory.createDatabaseTarget(config.getIngestTarget(), config, config.recreateIngestTarget(), allAPs);

        GeneratedAccessPoint[][] APpartitions = evenlyPartitionAPs(allAPs, config.getIngestThreadCount());
        boolean firstCreatedTarget = true;
        //TODO: Might need some functionality to ensure that the ingest-threads are kept similar in speeds.
        //      Otherwise one ingest thread might end up several hours/days in front of the others which then makes
        //      any queries for 'recent' data too easy. Or I could make queries for 'recent' data be the recency of
        //      the slowest thread (currently it follows the fastest one).
        for(int i = 0; i < config.getIngestThreadCount(); i++) {
            Random ingestRngForThread = new Random(ingestRngSource.nextInt());
            ITarget ingestTarget;
            if (config.useSharedIngestInstance()) {
                ingestTarget = ingestTargets[0];
            } else {
                // Only recreate the ingest-target during the first initialization. Avoids churn on the database/target.
                //   Probably doesn't really matter since we create all the instances before ingest begins.
                boolean recreate = false;
                if (firstCreatedTarget) {
                    recreate = config.recreateIngestTarget();
                    firstCreatedTarget = false;
                }
                ingestTarget = DatabaseTargetFactory.createDatabaseTarget(config.getIngestTarget(), config, recreate, allAPs);

                if(config.DEBUG_isPartitionLockstepEnabled()) ingestTarget = new MultiTarget(ingestTarget, new PartitionLockstepIngestionController(config, DEBUG_partitionLockstepChannel));

                ingestTargets[i] = ingestTarget;
            }

            // If ingestion runs alongside querying then we stop ingestion when we're done querying.
            // However, if querying isn't running then we might want to stop ingestion at some specific date.
            LocalDate ingestEndDate = config.isQueryingEnabled() ? LocalDate.MAX : config.getIngestEndDate();
            ingestRunnables[i] = new IngestRunnable(config, APpartitions[i], seedEntries, ingestRngForThread, ingestTarget, dateComm, i, ingestEndDate, doDirectComm);
        }

        for(int i = 0; i < ingestTasks.length; i++){
            ingestTasks[i] = threadPoolIngest.submit(ingestRunnables[i]);
        }
    }

    private void stopIngestion() throws Exception {
        // Once it's time to stop ingestion, first tell ingestion to stop gracefully.
        for(IngestRunnable ingestRunnable : ingestRunnables){
            ingestRunnable.stop();
        }

        // Then Wait for ingestion to shutdown gracefully
        for (Future ingestTask : ingestTasks) {
            ingestTask.get();
        }

        for(ITarget ingestTarget : ingestTargets){
            ingestTarget.close();
        }
        threadPoolIngest.shutdown();
    }

    private SeedData parseMapData(ConfigFile config) throws IOException {
        Logger.LOG("Loading seed data.");
        SeedData seedData = LoaderFacade.LoadSeedData(config);
        Logger.LOG("Loaded seed data for " + seedData.floorMetadata.length + " floors, with " + seedData.seedEntries.loadedEntries.size() + " days of source data.");
        return seedData;
    }

    private GeneratedFloor[] generateFloorData(ConfigFile config, Random rng, SeedData parsedData) throws Exception {
        Logger.LOG("Generating floors.");
        GeneratedFloor[] generatedFloors = GeneratorFacade.GenerateFloors(config, parsedData, rng);

        GeneratedAccessPoint[] allAPs = GeneratedFloor.allAPsOnFloors(generatedFloors);
        Logger.LOG(String.format("Generated metadata for %s floors and %s APs.", generatedFloors.length, allAPs.length));

        Logger.LOG("Setting up targets.");
        ITarget target = new BaseTarget();
        try{
            for(DBTargets configTarget : config.saveGeneratedDataTargets()){
                target = new MultiTarget(target, DatabaseTargetFactory.createDatabaseTarget(configTarget, config, true, allAPs));
            }

            PreciseTimer generationTimer = new PreciseTimer();
            CountTarget counter = new CountTarget();
            target = new MultiTarget(target, counter);
            Logger.LOG("Generating data.");
            generationTimer.start();
            DataGenerator.Generate(allAPs, parsedData.seedEntries, config.getGeneratorStartDate(), config.getGeneratorEndDate(), rng, target, config);
            double timeSpent = generationTimer.elapsedSeconds();
            Logger.LOG(String.format("Generated %s entries in %.2f sec.", counter.getCount(), timeSpent));
            if(config.doLoggingToCSV()) CSVLogger.GeneralLogger.createOrGetInstance().write("Main", String.format("Generated %s entries in %.2f sec.", counter.getCount(), timeSpent));

            if(target.shouldStopEarly()){
                Logger.LOG("POTENTIAL ERROR: Generation was stopped early.");
            }
        } finally {
            target.close();
        }

        if(config.DEBUG_createPrecomputedTables()){
            Logger.LOG("DEBUG: Filling precomputation tables.");
            InfluxPrecomputationInsights.ComputeTotals(config.getGeneratorGenerationSamplerate(), generatedFloors, config);
        }

        if(config.doSerialization()){
            Logger.LOG("Serializing floor and rng.");
            serializeData(generatedFloors, rng, config);
        }

        return generatedFloors;
    }

    private void serializeData(GeneratedFloor[] generatedFloors, Random rng, ConfigFile config) throws IOException{
        try(FileOutputStream outFile = new FileOutputStream(Paths.get(config.getSerializationPath(), "floors.ser").toString());
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(generatedFloors);
        }
        try(FileOutputStream outFile = new FileOutputStream(Paths.get(config.getSerializationPath(), "random.ser").toString());
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(rng);
        }
    }

    private GeneratedFloor[] deserializeFloor(ConfigFile config) throws IOException, ClassNotFoundException{
        GeneratedFloor[] data;
        try(FileInputStream inFile = new FileInputStream(Paths.get(config.getSerializationPath(), "floors.ser").toString());
            ObjectInputStream inStream = new ObjectInputStream(inFile)){
            data = (GeneratedFloor[]) inStream.readObject();
        }
        return data;
    }

    private Random deserializeRandom(ConfigFile config) throws IOException, ClassNotFoundException{
        Random rng;
        try(FileInputStream inFile = new FileInputStream(Paths.get(config.getSerializationPath(), "random.ser").toString());
            ObjectInputStream inStream = new ObjectInputStream(inFile)){
            rng = (Random) inStream.readObject();
        }
        return rng;
    }

    private GeneratedAccessPoint[][] evenlyPartitionAPs(GeneratedAccessPoint[] allAPs, int partitions){
        List<List<GeneratedAccessPoint>> results = new ArrayList<>(partitions);
        for(int i = 0; i < partitions; i++){
            results.add(new ArrayList<>());
        }
        for(int i = 0; i < allAPs.length; i++){
            results.get(i % partitions).add(allAPs[i]);
        }

        GeneratedAccessPoint[][] out = new GeneratedAccessPoint[partitions][];
        for(int i = 0; i < partitions; i++){
            out[i] = results.get(i).toArray(new GeneratedAccessPoint[0]);
        }

        return out;
    }

    private IQueries instantiateQueries(ConfigFile config){
        if(config.DEBUG_isPartitionLockstepEnabled()){
            if(config.DEBUG_partitionLockstepExplainAnalyzeTimescale() && config.getQueriesTarget() == DBTargets.TIMESCALE){
                if(config.getSchema() == SchemaFormats.COLUMN){
                    return new PartitionLockstepTimescaleDetailedColumnQueries(DEBUG_partitionLockstepChannel);
                } else {
                    throw new IllegalStateException("Debug option detailed partition-lockstep not implemented for this timescale schema: " + config.getSchema());
                }
            } else {
                return new PartitionLockstepQueryProxy(DatabaseQueriesFactory.createQueriesInstance(config), DEBUG_partitionLockstepChannel);
            }
        }

        return DatabaseQueriesFactory.createQueriesInstance(config);
    }
}
