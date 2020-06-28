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
import Benchmark.Ingestion.IngestOrchestrator;
import Benchmark.Queries.QueryOrchestrator;
import Benchmark.SeedLoader.LoaderFacade;
import Benchmark.Generator.Targets.*;
import Benchmark.SeedLoader.SeedData;
import Benchmark.Queries.IQueries;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.Random;
import java.util.Set;

/**
 * The entry-point of the benchmark program.
 */
public class Entrypoint {
    //TODO: Replace the argument handling here with a proper CLI argument library.
    //      (such as pico-cli used in the seed data generator)
    public static void main(String[] args) throws Exception {
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
            throw e;
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

        new Entrypoint().run(config);
    }

    private PartitionLockstepChannel DEBUG_partitionLockstepChannel;

    public void run(ConfigFile config) throws Exception {
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

            if(config.doSerialization()){
                Logger.LOG("Serializing floor and rng.");
                Serializer.serializeFloor(generatedFloors, config.getSerializationPath());
                Serializer.serializeRandom(rng, config.getSerializationPath());
            }
        } else {
            assert config.doSerialization() : "If the generator isn't enabled then we must get our metadata from previously serialized data.";
            // Load the data from previous run
            Logger.LOG("Deserializing floor.");
            generatedFloors = Serializer.deserializeFloor(config.getSerializationPath());
            Logger.LOG(String.format("Deserialized metadata for %s floors and %s APs", generatedFloors.length, GeneratedFloor.allAPsOnFloors(generatedFloors).length));
            Logger.LOG("Deserializing rng.");
            rng = Serializer.deserializeRandom(config.getSerializationPath());
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

        IngestOrchestrator ingestOrchestrator = null;
        if(config.isIngestionEnabled()){
            assert parsedData != null;
            Logger.LOG("Starting ingestion.");
            ingestOrchestrator = new IngestOrchestrator(config);
            ingestOrchestrator.prepareIngestion(generatedFloors, parsedData.seedEntries, dateComm, ingestRngSource,
                    !config.doDateCommunicationByQueryingDatabase(), DEBUG_partitionLockstepChannel);
            ingestOrchestrator.startIngestion();
            Logger.LOG("Ingestion started.");
        }

        QueryOrchestrator queryOrchestrator = null;
        if(config.isQueryingEnabled()){
            Logger.LOG("Starting queries.");
            queryOrchestrator = new QueryOrchestrator(config, () -> instantiateQueries(config));
            queryOrchestrator.prepareQuerying(generatedFloors, queryRngSource, dateComm);
            queryOrchestrator.startQuerying();
            Logger.LOG("Queries started.");
        }

        if(config.isQueryingEnabled()){
            // Wait for queries to finish, since they have a specified duration
            assert queryOrchestrator != null;
            queryOrchestrator.waitUntilQuerythreadsFinish();
        } else if(config.isIngestionEnabled()) {
            assert ingestOrchestrator != null;
            // If we aren't running the queries, but are running ingestion, then we need some other way to determine
            //   how long to run ingestion for. Otherwise they'll just stop immediately.
            boolean timeDuration = config.getIngestionStandaloneDuration() > 0;

            CoarseTimer timer = new CoarseTimer();
            timer.start();
            while(!timeDuration || timer.elapsedSeconds() < config.getIngestionStandaloneDuration()){
                boolean allDone = ingestOrchestrator.hasAllIngestThreadsFinished();
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
            assert ingestOrchestrator != null;
            ingestOrchestrator.shutdownIngestion();
        }

        if(config.isQueryingEnabled()) {
            assert queryOrchestrator != null;
            queryOrchestrator.shutdownQuerying();
        }

        if(config.doLoggingToCSV()){
            CSVLogger.GeneralLogger.createOrGetInstance().setDone();
            Logger.LOG("Writing to CSV files.");
            CSVLogger.writeAllToDisk(config);
        }

        Logger.LOG("Done.");
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

        return generatedFloors;
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
