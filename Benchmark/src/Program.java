import Benchmark.Analysis.Precomputation;
import Benchmark.Config.ConfigFile;
import Benchmark.Generator.Floor;
import Benchmark.Generator.Generator;
import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.MapData;
import Benchmark.Generator.Targets.*;
import Benchmark.MapParser;

import java.io.IOException;
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

        if(config.generatedata()){
            System.out.println(LocalTime.now().toString() + ": Parsing map");
            MapData parsedData = MapParser.ParseMap(config.idmap(), config.mapfolder());
            System.out.println(LocalTime.now().toString() + ": Generating floors");
            Floor[] generatedFloors = Generator.Generate(config.scale(), rng);
            System.out.println(LocalTime.now().toString() + ": Assigning floors to IDs");
            Generator.AssignFloorsToIDs(generatedFloors, parsedData, config.keepFloorAssociations());
            System.out.println(LocalTime.now().toString() + ": Preparing for generation");
            Generator.PrepareDataForGeneration(generatedFloors, parsedData);

            System.out.println(LocalTime.now().toString() + ": Setting up targets");
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
                System.out.println(LocalTime.now().toString() + ": Generating data");
                DataGenerator.Generate(config.generationinterval(), config.entryinterval(), generatedFloors, parsedData, config.startDate(), config.endDate(), config.scale(), rng, target);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                target.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


            System.out.println(LocalTime.now().toString() + ": DEBUG: Filling precomputation tables");
            try {
                Precomputation.ComputeTotals(config.generationinterval(), generatedFloors, config);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // Load the generated floors and parsed data from previous run
        }

        if(config.runqueries()){

        }

        System.out.println(LocalTime.now().toString() + ": Done.");
    }
}
