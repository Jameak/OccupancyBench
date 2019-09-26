import Benchmark.Generator.Floor;
import Benchmark.Generator.Generator;
import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.MapData;
import Benchmark.Generator.Targets.*;
import Benchmark.MapParser;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Random;

public class Program {
    public static void main(String[] args) {
        boolean generatedata = true;
        boolean savedatatodisk = true;
        boolean savedataintoDB = false;
        double scale = 1;
        String idMapPath = "get from argument";
        String mapFolderPath = "get from argument";
        String pathToSaveFolder = "get from argument";
        int seed = 1234; // Get from argument
        Random rng = new Random(seed);
        int generationIntervalInSeconds = 60;
        int intervalBetweenEntriesInLoadedData = 60;
        LocalDate startDate = LocalDate.of(2019, 1, 1);
        LocalDate endDate = LocalDate.of(2019, 3, 31);

        String influxdbUrl = "get from argument";
        String influxdbUsername = "get from argument";
        String influxdbPassword = "get from argument";
        String influxdbName = "get from argument";
        String influxdbTablename = "get from argument";

        boolean runBenchmark = false;
        boolean ingest = false;

        if(generatedata){
            MapData parsedData = MapParser.ParseMap(idMapPath, mapFolderPath);
            Floor[] generatedFloors = Generator.Generate(scale, rng);
            Generator.AssignFloorsToIDs(generatedFloors, parsedData);
            Generator.PrepareDataForGeneration(generatedFloors, parsedData);

            ITarget target = new BaseTarget();
            if(savedatatodisk){
                try {
                    target = new MultiTarget(target, new FileTarget(pathToSaveFolder, "output.csv"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(savedataintoDB){
                try {
                    target = new MultiTarget(target, new InfluxTarget(influxdbUrl, influxdbUsername, influxdbPassword, influxdbName, influxdbTablename));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                DataGenerator.Generate(generationIntervalInSeconds, intervalBetweenEntriesInLoadedData, generatedFloors, parsedData, startDate, endDate, scale, rng, target);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                target.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // Load the generated floors and parsed data from previous run
        }


        if(runBenchmark){

        }
    }
}
