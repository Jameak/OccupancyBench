import Benchmark.Analysis.Precomputation;
import Benchmark.Config.ConfigFile;
import Benchmark.Generator.*;
import Benchmark.Generator.Ingest.IngestRunnable;
import Benchmark.Generator.Targets.*;
import Benchmark.Logger;
import Benchmark.MapParser;
import Benchmark.Queries.QueryRunnable;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

        try {
            new Program().run(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run(ConfigFile config) throws Exception{
        Random rng = new Random(config.seed());
        Logger logger = new Logger();

        MapData parsedData;
        Floor[] generatedFloors;
        if(config.generatedata()){
            logger.log("Parsing map.");
            parsedData = MapParser.ParseMap(config.idmap(), config.mapfolder());
            logger.log("Generating floors.");
            generatedFloors = Generator.Generate(config.scale(), rng);
            logger.log("Assigning floors to IDs.");
            Generator.AssignFloorsToIDs(generatedFloors, parsedData, config.keepFloorAssociations());
            logger.log("Preparing for generation.");
            Generator.PrepareDataForGeneration(generatedFloors, parsedData);

            logger.log("Setting up targets.");
            ITarget target = new BaseTarget();
            if(config.saveToDisk()){
                target = new MultiTarget(target, new FileTarget(config.toDiskFolder(), config.toDiskFilename()));
            }
            if(config.saveToInflux()){
                target = new MultiTarget(target, new InfluxTarget(config.influxUrl(), config.influxUsername(), config.influxPassword(), config.influxDBName(), config.influxTable()));
            }

            logger.log("Generating data");
            AccessPoint[] allAPs = allAPs(generatedFloors);
            DataGenerator.Generate(allAPs, parsedData, config.startDate(), config.endDate(), rng, target, config);

            if(target.shouldStopEarly()){
                logger.log("POTENTIAL ERROR: Entry generation was stopped early.");
            }

            target.close();

            if(config.createDebugTables()){
                logger.log("DEBUG: Filling precomputation tables.");
                Precomputation.ComputeTotals(config.generationinterval(), generatedFloors, config);
            }

            if(config.serialize()){
                //TODO: Also serialize the instance of random so we can continue from where we left off.
                logger.log("Serializing floors.");
                serializeGeneratedData(generatedFloors, config);
            }
        } else {
            // Load the generated floors and parsed data from previous run
            logger.log("Parsing map.");
            parsedData = MapParser.ParseMap(config.idmap(), config.mapfolder());

            logger.log("Deserializing floors.");
            generatedFloors = deserializeGeneratedData(config);
        }

        ExecutorService threadPoolIngest = Executors.newFixedThreadPool(config.threadsIngest());
        ExecutorService threadPoolQueries = Executors.newFixedThreadPool(config.threadsQueries());
        logger.setDirectly(config.endDate(), LocalTime.of(0,0,0));

        Future[] ingestTasks = new Future[config.threadsIngest()];
        IngestRunnable[] ingestRunnables = new IngestRunnable[config.threadsIngest()];
        ITarget ingestTarget = null;
        if(config.ingest()){
            logger.log("Starting ingestion.");
            ingestTarget = new InfluxTarget(config.influxUrl(), config.influxUsername(), config.influxPassword(), config.influxDBName(), config.influxTable());
            AccessPoint[][] APpartitions = evenlyPartitionAPs(allAPs(generatedFloors), config.threadsIngest());
            //TODO: Might need some functionality to ensure that the ingest-threads are kept similar in speeds.
            //      Otherwise one ingest thread might end up several hours/days in front of the others which then makes
            //      any queries for 'recent' data too easy. Or I could make queries for 'recent' data be the recency of
            //      the slowest thread (currently it follows the fastest one).
            for(int i = 0; i < config.threadsIngest(); i++){
                Random ingestRng = new Random(rng.nextInt());
                ingestRunnables[i] = new IngestRunnable(config, APpartitions[i], parsedData, ingestRng, ingestTarget, logger, "Ingest " + i);
                ingestTasks[i] = threadPoolIngest.submit(ingestRunnables[i]);
            }
            logger.log("Ingestion started.");
        }

        Future[] queryTasks = new Future[config.threadsQueries()];
        if(config.runqueries()){
            logger.log("Starting queries.");
            for(int i = 0; i < config.threadsQueries(); i++){
                Random queryRng = new Random(rng.nextInt());
                QueryRunnable queryRunnable = new QueryRunnable(config, queryRng, logger, generatedFloors, "Query " + i);
                queryTasks[i] = threadPoolQueries.submit(queryRunnable);
            }
            logger.log("Queries started.");
        }

        // First, wait for queries to finish, since they have a specified duration
        for(Future queryTask : queryTasks){
            if(queryTask != null) queryTask.get();
        }

        if(config.runqueries()){
            // Once queries have finished, tell ingestion to stop.
            for(IngestRunnable ingestRunnable : ingestRunnables){
                if(ingestRunnable != null) ingestRunnable.stop();
            }
        } else if(config.ingest()) {
            // If we aren't running the queries, but are running ingestion, then we need some other way to determine
            //   how long to run ingestion for. Otherwise they'll just stop immediately.
            Thread.sleep(config.durationStandalone() * 1000);

            for(IngestRunnable ingestRunnable : ingestRunnables){
                if(ingestRunnable != null) ingestRunnable.stop();
            }
        }

        // Wait for ingestion tasks to shutdown
        for (Future ingestTask : ingestTasks) {
            if (ingestTask != null) ingestTask.get();
        }

        if(ingestTarget != null){
            ingestTarget.close();
        }

        threadPoolIngest.shutdown();
        threadPoolQueries.shutdown();
        logger.log("Done.");
    }

    public void serializeGeneratedData(Floor[] generatedFloors, ConfigFile config) throws IOException{
        try(FileOutputStream outFile = new FileOutputStream(config.serializePath());
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(generatedFloors);
        }
    }

    public Floor[] deserializeGeneratedData(ConfigFile config) throws IOException, ClassNotFoundException{
        Floor[] data = null;
        try(FileInputStream inFile = new FileInputStream(config.serializePath());
            ObjectInputStream inStream = new ObjectInputStream(inFile)){
            data = (Floor[]) inStream.readObject();
        }
        return data;
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
}
