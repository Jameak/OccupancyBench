package Benchmark.Generator;

import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

import java.util.*;
import java.util.function.IntFunction;

/**
 * Generator of floors and the access-points that they contain.
 */
public class FloorGenerator {
    private static String GenerateAPName(Random rng){
        // TODO: Make sure that the same AP-name isn't generated twice. The chance of that happening for my case is almost non-existent though...
        String hex = String.format("%06X", rng.nextInt());
        return "AP-" + hex.substring(0, 6);
    }

    public static Floor GenerateGroundlevelFloor(Random rng){
        REDACTED DATA
    }

    public static Floor GenerateFloor(int floorNumber, Random rng){
        REDACTED DATA
    }
}
