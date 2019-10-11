import Benchmark.Analysis.Precomputation;
import Benchmark.Config.ConfigFile;
import Benchmark.Generator.Floor;
import Benchmark.Generator.Generator;
import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.Ingest.IngestRunnable;
import Benchmark.Generator.MapData;
import Benchmark.Generator.Targets.*;
import Benchmark.Logger;
import Benchmark.MapParser;
import Benchmark.Queries.QueryRunnable;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.Random;

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

        new Program().run(config);
    }

    public void run(ConfigFile config){
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
                try {
                    target = new MultiTarget(target, new FileTarget(config.toDiskFolder(), config.toDiskFilename()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(config.saveToInflux()){
                try {
                    target = new MultiTarget(target, new InfluxTarget(config.influxUrl(), config.influxUsername(), config.influxPassword(), config.influxDBName(), config.influxTable()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                logger.log("Generating data");
                DataGenerator.Generate(generatedFloors, parsedData, config.startDate(), config.endDate(), rng, target, config);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if(target.shouldStopEarly()){
                logger.log("POTENTIAL ERROR: Entry generation was stopped early.");
            }

            try {
                target.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(config.createDebugTables()){
                logger.log("DEBUG: Filling precomputation tables.");
                try {
                    Precomputation.ComputeTotals(config.generationinterval(), generatedFloors, config);
                } catch (IOException e) {
                    logger.log("Failed to fill debug tables.");
                    e.printStackTrace();
                }
            }

            if(config.serialize()){
                //TODO: Also serialize the instance of random so we can continue from where we left off.
                logger.log("Serializing floors.");
                serializeGeneratedData(generatedFloors, config, logger);
            }
        } else {
            // Load the generated floors and parsed data from previous run
            logger.log("Parsing map.");
            parsedData = MapParser.ParseMap(config.idmap(), config.mapfolder());

            logger.log("Deserializing floors.");
            generatedFloors = deserializeGeneratedData(config, logger);
        }

        logger.setDirectly(config.endDate(), LocalTime.of(0,0,0));

        Thread ingestThread = null;
        IngestRunnable ingestRunnable = null;
        ITarget ingestTarget = null;
        if(config.ingest()){
            logger.log("Starting ingestion.");
            Random ingestRng = new Random(rng.nextInt());
            try {
                ingestTarget = new InfluxTarget(config.influxUrl(), config.influxUsername(), config.influxPassword(), config.influxDBName(), config.influxTable());
            } catch (IOException e) {
                e.printStackTrace();
            }
            ingestRunnable = new IngestRunnable(config, generatedFloors, parsedData, ingestRng, ingestTarget, logger);
            ingestThread = new Thread(ingestRunnable);
            ingestThread.start(); //TODO: If I multithread ingestion then this needs to be submitted to a thread-pool.
            logger.log("Ingestion started.");
        }

        Thread queryThread = null;
        QueryRunnable queryRunnable = null;
        if(config.runqueries()){
            logger.log("Starting queries.");
            Random queryRng = new Random(rng.nextInt());
            queryRunnable = new QueryRunnable(config, queryRng, ingestRunnable, logger, generatedFloors);
            queryThread = new Thread(queryRunnable);
            queryThread.start(); //TODO: If I multithread queries then this needs to be submitted to a thread-pool.
            logger.log("Queries started.");
        }

        if(ingestThread != null){
            try {
                ingestThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(queryThread != null){
            try {
                queryThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(ingestTarget != null){
            try {
                ingestTarget.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        logger.log("Done.");
    }

    public void serializeGeneratedData(Floor[] generatedFloors, ConfigFile config, Logger logger){
        try(FileOutputStream outFile = new FileOutputStream(config.serializePath());
            ObjectOutputStream outStream = new ObjectOutputStream(outFile)){
            outStream.writeObject(generatedFloors);
        } catch (IOException e){
            logger.log("Serialization failed.");
            e.printStackTrace();
        }
    }

    public Floor[] deserializeGeneratedData(ConfigFile config, Logger logger){
        Floor[] data = null;
        try(FileInputStream inFile = new FileInputStream(config.serializePath());
            ObjectInputStream inStream = new ObjectInputStream(inFile)){
            data = (Floor[]) inStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.log("Deserialization failed.");
            e.printStackTrace();
        }
        return data;
    }
}
