import Benchmark.Analysis.Precomputation;
import Benchmark.CoarseTimer;
import Benchmark.Config.ConfigFile;
import Benchmark.DateCommunication;
import Benchmark.Generator.*;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Generator.Ingest.IngestRunnable;
import Benchmark.Loader.MapData;
import Benchmark.Generator.Targets.*;
import Benchmark.Logger;
import Benchmark.Loader.MapParser;
import Benchmark.Queries.InfluxQueries;
import Benchmark.Queries.Queries;
import Benchmark.Queries.QueryRunnable;
import Benchmark.Queries.TimescaleQueries;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The entry-point of the benchmark program.
 */
public class Program {
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
            config = ConfigFile.load(args[0]);
        } catch (IOException e) {
            System.out.println("Cannot load config from path: " + args[0] + "\n\n");
            e.printStackTrace();
            return;
        }

        if(!config.isValidConfig()){
            System.out.println("Config has invalid values: " + config.getValidationError());
            System.out.println("Aborting.");
            return;
        }

        try {
            new Program().run(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // INGESTION 'GLOBALS'
    private ExecutorService threadPoolIngest;
    private Future[] ingestTasks;
    private IngestRunnable[] ingestRunnables;
    private ITarget[] ingestTargets;

    // QUERY 'GLOBALS'
    private ExecutorService threadPoolQueries;

    public void run(ConfigFile config) throws Exception{
        assert config.isValidConfig();
        Random rng = new Random(config.getSeed());
        DateCommunication dateComm = new DateCommunication();

        MapData parsedData = null;
        if (config.isGeneratorEnabled() || config.isIngestionEnabled()) {
            parsedData = parseMapData(config);
        }

        Floor[] generatedFloors;
        if(config.isGeneratorEnabled()){
            generatedFloors = generateFloorData(config, rng, parsedData);
        } else {
            assert config.doSerialization();
            // Load the data from previous run
            Logger.LOG("Deserializing floor.");
            generatedFloors = deserializeFloor(config);
            Logger.LOG(String.format("Floor data: %s floors, %s APs", generatedFloors.length, allAPs(generatedFloors).length));
            Logger.LOG("Deserializing rng.");
            rng = deserializeRandom(config);
        }

        dateComm.setInitialDate(config.getGeneratorEndDate(), LocalTime.of(0,0,0));

        if(config.isIngestionEnabled()){
            Logger.LOG("Starting ingestion.");
            startIngestion(config, generatedFloors, parsedData, dateComm, rng);
            Logger.LOG("Ingestion started.");
        }

        Future[] queryTasks = null;
        if(config.isQueryingEnabled()){
            Logger.LOG("Starting queries.");
            queryTasks = startQueries(config, dateComm, rng, generatedFloors);
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

        if(config.isIngestionEnabled()){
            stopIngestion();
        }

        if(config.isQueryingEnabled()) threadPoolQueries.shutdown();
        Logger.LOG("Done.");
    }

    private Future[] startQueries(ConfigFile config, DateCommunication dateComm, Random rng, Floor[] generatedFloors) {
        assert config.getQueriesThreadCount() > 0;
        threadPoolQueries = Executors.newFixedThreadPool(config.getQueriesThreadCount());

        Queries queryInstance = null;
        if(config.useSharedQueriesInstance()) queryInstance = createQueryTargetInstance(config);

        Future[] queryTasks = new Future[config.getQueriesThreadCount()];
        for(int i = 0; i < config.getQueriesThreadCount(); i++){
            Random queryRng = new Random(rng.nextInt());
            if(!config.useSharedQueriesInstance()) queryInstance = createQueryTargetInstance(config);

            QueryRunnable queryRunnable = new QueryRunnable(config, queryRng, dateComm, generatedFloors, queryInstance, "Query " + i);
            queryTasks[i] = threadPoolQueries.submit(queryRunnable);
        }
        return queryTasks;
    }

    private void startIngestion(ConfigFile config, Floor[] generatedFloors, MapData parsedData, DateCommunication dateComm, Random rng) throws IOException, SQLException {
        assert config.getIngestThreadCount() > 0;
        threadPoolIngest = Executors.newFixedThreadPool(config.getIngestThreadCount());
        ingestTasks = new Future[config.getIngestThreadCount()];
        ingestRunnables = new IngestRunnable[config.getIngestThreadCount()];

        ingestTargets = new ITarget[config.useSharedIngestInstance() ? 1 : config.getIngestThreadCount()];
        if(config.useSharedIngestInstance()) ingestTargets[0] = createTargetInstance(config.getIngestTarget(), config, config.recreateIngestTarget());

        AccessPoint[][] APpartitions = evenlyPartitionAPs(allAPs(generatedFloors), config.getIngestThreadCount());
        boolean firstCreatedTarget = true;
        //TODO: Might need some functionality to ensure that the ingest-threads are kept similar in speeds.
        //      Otherwise one ingest thread might end up several hours/days in front of the others which then makes
        //      any queries for 'recent' data too easy. Or I could make queries for 'recent' data be the recency of
        //      the slowest thread (currently it follows the fastest one).
        for(int i = 0; i < config.getIngestThreadCount(); i++) {
            Random ingestRng = new Random(rng.nextInt());
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
                ingestTarget = createTargetInstance(config.getIngestTarget(), config, recreate);
                ingestTargets[i] = ingestTarget;
            }

            // If ingestion runs alongside querying then we stop ingestion when we're done querying.
            // However, if querying isn't running then we might want to stop ingestion at some specific date.
            LocalDate ingestEndDate = config.isQueryingEnabled() ? LocalDate.MAX : config.getIngestEndDate();
            ingestRunnables[i] = new IngestRunnable(config, APpartitions[i], parsedData, ingestRng, ingestTarget, dateComm, "Ingest " + i, ingestEndDate);
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

    private MapData parseMapData(ConfigFile config) throws IOException{
        Logger.LOG("Parsing map.");
        return MapParser.ParseMap(config.getGeneratorIdmap(), config.getGeneratorMapfolder());
    }

    private Floor[] generateFloorData(ConfigFile config, Random rng, MapData parsedData) throws Exception {
        Floor[] generatedFloors;
        Logger.LOG("Generating floors.");
        generatedFloors = Generator.Generate(config.getGeneratorScale(), rng);
        Logger.LOG("Assigning floors to IDs.");
        Generator.AssignFloorsToIDs(generatedFloors, parsedData, config.keepFloorAssociationsForGenerator());
        Logger.LOG("Preparing for generation.");
        Generator.PrepareDataForGeneration(generatedFloors, parsedData);

        Logger.LOG("Setting up targets.");
        ITarget target = null;
        try{
            target = new BaseTarget();
            for(ConfigFile.Target configTarget : config.saveGeneratedDataTargets()){
                target = new MultiTarget(target, createTargetInstance(configTarget, config, true));
            }

            Logger.LOG("Generating data");
            AccessPoint[] allAPs = allAPs(generatedFloors);
            DataGenerator.Generate(allAPs, parsedData, config.getGeneratorStartDate(), config.getGeneratorEndDate(), rng, target, config);

            if(target.shouldStopEarly()){
                Logger.LOG("POTENTIAL ERROR: Generation was stopped early.");
            }
        } finally {
            if(target != null) target.close();
        }

        if(config.generatorCreateDebugTables()){
            Logger.LOG("DEBUG: Filling precomputation tables.");
            Precomputation.ComputeTotals(config.getGeneratorGenerationInterval(), generatedFloors, config);
        }

        if(config.doSerialization()){
            Logger.LOG("Serializing floor and rng.");
            serializeData(generatedFloors, rng, config);
        }

        return generatedFloors;
    }

    private void serializeData(Floor[] generatedFloors, Random rng, ConfigFile config) throws IOException{
        try(FileOutputStream outFile = new FileOutputStream(Paths.get(config.getSerializationPath(), "floors.ser").toString());
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(generatedFloors);
        }
        try(FileOutputStream outFile = new FileOutputStream(Paths.get(config.getSerializationPath(), "random.ser").toString());
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(rng);
        }
    }

    private Floor[] deserializeFloor(ConfigFile config) throws IOException, ClassNotFoundException{
        Floor[] data;
        try(FileInputStream inFile = new FileInputStream(Paths.get(config.getSerializationPath(), "floors.ser").toString());
            ObjectInputStream inStream = new ObjectInputStream(inFile)){
            data = (Floor[]) inStream.readObject();
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

    private AccessPoint[] allAPs(Floor[] generatedFloors){
        List<AccessPoint> APs = new ArrayList<>();
        for(Floor floor : generatedFloors){
            APs.addAll(Arrays.asList(floor.getAPs()));
        }
        return APs.toArray(new AccessPoint[0]);
    }

    private AccessPoint[][] evenlyPartitionAPs(AccessPoint[] allAPs, int partitions){
        List<List<AccessPoint>> results = new ArrayList<>(partitions);
        for(int i = 0; i < partitions; i++){
            results.add(new ArrayList<>());
        }
        for(int i = 0; i < allAPs.length; i++){
            results.get(i % partitions).add(allAPs[i]);
        }

        AccessPoint[][] out = new AccessPoint[partitions][];
        for(int i = 0; i < partitions; i++){
            out[i] = results.get(i).toArray(new AccessPoint[0]);
        }

        return out;
    }

    private ITarget createTargetInstance(ConfigFile.Target target, ConfigFile config, boolean recreate) throws IOException, SQLException {
        switch (target){
            case INFLUX:
                return new InfluxTarget(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword(),
                        config.getInfluxDBName(), config.getInfluxTable(), recreate, config.getInfluxBatchsize(),
                        config.getInfluxFlushtime(), config.getGeneratorGranularity());
            case FILE:
                // Not supported for ingestion. Valid config files shouldn't contain FILE as the chosen ingest-target.
                return new FileTarget(config.getGeneratorDiskTarget(), config.getGeneratorGranularity());
            case TIMESCALE:
                return new TimescaleTarget(config.getTimescaleHost(), config.getTimescaleDBName(),
                        config.getTimescaleUsername(), config.getTimescalePassword(), config.getTimescaleTable(),
                        recreate, config.getTimescaleBatchSize(), config.reWriteBatchedTimescaleInserts(), config.getGeneratorGranularity());
            default:
                assert false : "New ingestion target must have been added, but target-switch wasn't updated";
                throw new IllegalStateException("Unknown ingestion target: " + config.getIngestTarget());
        }
    }

    private Queries createQueryTargetInstance(ConfigFile config){
        switch (config.getQueriesTarget()){
            case INFLUX:
                return new InfluxQueries();
            case TIMESCALE:
                return new TimescaleQueries();
            case FILE:
                assert false;
                throw new IllegalStateException("Unsupported query target: " + config.getQueriesTarget());
            default:
                assert false : "New query target must have been added, but query-switch wasn't updated";
                throw new IllegalStateException("Unknown query target: " + config.getQueriesTarget());
        }
    }
}
