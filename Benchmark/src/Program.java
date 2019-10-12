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
            DataGenerator.Generate(generatedFloors, parsedData, config.startDate(), config.endDate(), rng, target, config);

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

        logger.setDirectly(config.endDate(), LocalTime.of(0,0,0));

        Thread ingestThread = null;
        IngestRunnable ingestRunnable = null;
        ITarget ingestTarget = null;
        if(config.ingest()){
            logger.log("Starting ingestion.");
            Random ingestRng = new Random(rng.nextInt());
            ingestTarget = new InfluxTarget(config.influxUrl(), config.influxUsername(), config.influxPassword(), config.influxDBName(), config.influxTable());
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
            ingestThread.join();
        }

        if(queryThread != null){
            queryThread.join();
        }

        if(ingestTarget != null){
            ingestTarget.close();
        }
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
}
