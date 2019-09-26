import Benchmark.Generator.Floor;
import Benchmark.Generator.Generator;
import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.MapData;
import Benchmark.MapParser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;

public class Program {
    public static void main(String[] args) {
        boolean generatedata = true;
        boolean savedatatodisk = true;
        boolean loaddataintoDB = false;
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

        boolean runBenchmark = false;
        boolean ingest = false;

        if(generatedata){
            MapData parsedData = MapParser.ParseMap(idMapPath, mapFolderPath);
            Floor[] generatedFloors = Generator.Generate(scale, rng);
            Generator.AssignFloorsToIDs(generatedFloors, parsedData);
            Generator.PrepareDataForGeneration(generatedFloors, parsedData);
            //TODO: Instead of generating data into a list that I then do stuff with (thereby keeping all generated
            // entries in memory) I should take in some generation-output so that I can either write it directly to
            // disk or to the database
            List<DataGenerator.GeneratedEntry> generatedData = DataGenerator.Generate(generationIntervalInSeconds, intervalBetweenEntriesInLoadedData, generatedFloors, parsedData, startDate, endDate, scale, rng);

            if(savedatatodisk){
                try(BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(pathToSaveFolder).resolve("output.csv").toString()))){
                    for(DataGenerator.GeneratedEntry entry : generatedData){
                        writer.write(entry.toString());
                        writer.write("\n");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            // Load the generated floors and parsed data from previous run
        }


        if(runBenchmark){

        }
    }
}
