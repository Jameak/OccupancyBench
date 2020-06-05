package Benchmark.Generator;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.SeedLoader.SeedData;

import java.util.Random;

public class GeneratorFacade {
    public static GeneratedFloor[] GenerateFloors(ConfigFile config, SeedData seedData, Random rng){
        GeneratedFloor[] floors = FloorGenerator.GenerateFloors(seedData.floorMetadata, config.getGeneratorScaleFactorFloors(), config.getGeneratorScaleFactorSensors(), rng);
        FloorGenerator.NormalizeSeedProbabilities(floors, seedData.seedEntries);
        return floors;
    }
}