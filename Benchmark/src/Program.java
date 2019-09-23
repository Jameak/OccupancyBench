import Generator.Floor;
import Generator.Generator;
import Generator.MapData;

import java.util.Random;

public class Program {
    public static void main(String[] args) {
        boolean generatedata = true;
        boolean savedatatodisk = true;
        boolean loaddataintoDB = false;
        double scale = 1;
        String idMapPath = "get from argument";
        String mapFolderPath = "get from argument";
        int seed = 1234; // Get from argument
        Random rng = new Random(seed);

        boolean runBenchmark = false;
        boolean ingest = false;

        if(generatedata){
            // DONE: Load probability data (might be missing some APs)
            // DONE: Generate floors
            // DONE: Assign probability AP ids to generated APs
            // NOT STARTED: Generate data points from probability map
            // NOTE: When generating data points based on probabilities with a non-standard scaling factor,
            //       we need to keep probabilities at 100%. Do this by summing all the probabilities and then
            //       divide each probability by the total.

            MapData parsedData = MapParser.ParseMap(idMapPath, mapFolderPath);
            Floor[] generatedFloors = Generator.Generate(scale, rng);
            Generator.AssignFloorsToIDs(generatedFloors, parsedData);
        }

        if(savedatatodisk){

        }

        if(runBenchmark){

        }
    }
}
