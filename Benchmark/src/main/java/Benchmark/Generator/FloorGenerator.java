package Benchmark.Generator;

import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.SeedLoader.Metadata.FloorMetadata;
import Benchmark.SeedLoader.Seeddata.Entry;
import Benchmark.SeedLoader.Seeddata.SeedEntries;

import java.util.*;

/**
 * Generator of floors and the access-points that they contain.
 */
public class FloorGenerator {
    private static String GenerateAPName(Random rng, Set<String> usedNames){
        String hex = String.format("%06X", rng.nextInt());
        String name = "AP-" + hex.substring(0, 6);
        // Avoid generating duplicate names. Probability of that happening is basically non-existent but... ehh.
        if(usedNames.contains(name)) return GenerateAPName(rng, usedNames);
        usedNames.add(name);
        return name;
    }

    /**
     * Generate floors (and the access-points on each floor) with the given scale using the given Random-instance.
     */
    public static GeneratedFloor[] GenerateFloors(FloorMetadata[] floorMetadata, double floorScaleFactor, double sensorScaleFactor, Random rng){
        assert floorScaleFactor > 0.0;
        int numberOfFloors = (int)Math.ceil(floorMetadata.length * floorScaleFactor);

        GeneratedFloor[] generatedFloors = new GeneratedFloor[numberOfFloors];

        List<FloorMetadata> repeatableFloors = new ArrayList<>();
        List<FloorMetadata> nonRepeatableFloor = new ArrayList<>();
        for(FloorMetadata floorData : floorMetadata){
            if(floorData.eligibleForAutoScaling) repeatableFloors.add(floorData);
            else nonRepeatableFloor.add(floorData);
        }

        Set<String> usedAPnames = new HashSet<>();
        int currFloorNumber = 0;
        for(FloorMetadata floorData : nonRepeatableFloor){
            if(currFloorNumber >= numberOfFloors) break;

            generatedFloors[currFloorNumber] = GenerateFloor(currFloorNumber, floorData, rng, usedAPnames, sensorScaleFactor);
            currFloorNumber++;
        }
        while(currFloorNumber < numberOfFloors){
            for(FloorMetadata floorData : repeatableFloors){
                if(currFloorNumber >= numberOfFloors) break;

                generatedFloors[currFloorNumber] = GenerateFloor(currFloorNumber, floorData, rng, usedAPnames, sensorScaleFactor);
                currFloorNumber++;
            }
        }

        return generatedFloors;
    }

    private static GeneratedFloor GenerateFloor(int floorNumber, FloorMetadata floorData, Random rng,
                                                Set<String> usedNames, double sensorScaleFactor){
        assert sensorScaleFactor > 0.0;
        int apsToGenerate = (int)Math.ceil(floorData.accessPointsOnFloor.size() * sensorScaleFactor);
        int generatedAPs = 0;

        GeneratedAccessPoint[] apsOnFloor = new GeneratedAccessPoint[apsToGenerate];
        while(generatedAPs < apsToGenerate){
            for(String seedName : floorData.accessPointsOnFloor){
                if(generatedAPs >= apsToGenerate) break;

                String newName = GenerateAPName(rng, usedNames);
                apsOnFloor[generatedAPs] = new GeneratedAccessPoint(newName, seedName);
                generatedAPs++;
            }
        }
        return new GeneratedFloor(floorNumber, apsOnFloor);
    }

    /**
     * Normalize the probabilities for each time-period (each entry) 100% to account for APs having been
     * assigned multiple times, or not being assigned at all.
     */
    public static void NormalizeSeedProbabilities(GeneratedFloor[] generatedFloors, SeedEntries seedEntries){
        Map<String, Integer> assignedSensorsAndAmountOfTimesAssigned = new HashMap<>();
        for(GeneratedFloor floor : generatedFloors){
            for(GeneratedAccessPoint AP : floor.getAPs()){
                AddToCount(assignedSensorsAndAmountOfTimesAssigned, AP.getOriginalName());
            }
        }

        for(Entry[] entriesAtSpecificTime : seedEntries.loadedEntries.values()){
            // Sum up all the probabilities of used APs at a specific timestamp entry.
            for(Entry entry : entriesAtSpecificTime){
                if(!entry.hasData()) continue;
                double totalAssignedProbability = 0.0;

                for(String apName : entry.getProbabilities().keySet()){
                    if(assignedSensorsAndAmountOfTimesAssigned.containsKey(apName)){
                        double probability = entry.getProbabilities().get(apName);
                        int count = assignedSensorsAndAmountOfTimesAssigned.get(apName);
                        totalAssignedProbability += probability * count;
                    }
                }

                // Update probabilities of each used AP at the entry by dividing with the total.
                // This normalizes the total probability to 1 (100%).
                for(String apName : entry.getProbabilities().keySet()){
                    if(assignedSensorsAndAmountOfTimesAssigned.containsKey(apName)){
                        double probability = entry.getProbabilities().get(apName);
                        double newprobability = probability / totalAssignedProbability;
                        entry.getProbabilities().put(apName, newprobability);
                    }
                }
            }
        }
    }

    private static void AddToCount(Map<String, Integer> count, String apName){
        if(!count.containsKey(apName)){
            count.put(apName, 0);
        }

        count.put(apName, count.get(apName) + 1);
    }
}
