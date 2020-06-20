package Generator;

import Generator.Generated.GeneratedAccessPoint;
import Generator.Generated.GeneratedCombination;
import Generator.Generated.GeneratedFloor;
import Generator.Generated.GeneratedMetadata;

import java.util.*;

public class MetadataGenerator {
    private final String AP_PREFIX = "AH-";
    private final int COMBINE_CHANCE = 5; // Valid range is 0 <= COMBINE_CHANCE < 100
    private final int IGNORE_CHANCE = 5; // Valid range is 0 <= IGNORE_CHANCE < 100

    private final int floorsToGen;
    private final int sensorsPerFloor;
    private final boolean specialFloor;
    private final Random rng;
    private final Set<String> usedNames = new HashSet<>();

    public MetadataGenerator(int floorsToGen, int sensorsPerFloor, boolean specialFloor, int rngSeed){
        assert floorsToGen > 0;
        assert sensorsPerFloor > 0;

        this.floorsToGen = floorsToGen;
        this.sensorsPerFloor = sensorsPerFloor;
        this.specialFloor = specialFloor;
        this.rng = new Random(rngSeed);
    }

    private String createName(){
        String hex = String.format("%06X", rng.nextInt());
        String name = AP_PREFIX + hex.substring(0, 6);

        if(usedNames.contains(name)) return createName();
        usedNames.add(name);
        return name;
    }

    /**
     * Generate metadata with the configuration specified in the constructor.
     */
    public GeneratedMetadata generate(){
        List<GeneratedFloor> floors = new ArrayList<>();
        if(specialFloor) floors.add(new GeneratedFloor("groundfloor", false));
        for(int i = 0; i < floorsToGen; i++){
            floors.add(new GeneratedFloor("floor"+i, true));
        }

        List<GeneratedAccessPoint> accessPoints = new ArrayList<>();
        for(GeneratedFloor floor : floors){
            if(floor.eligibleForScaling){
                for(int i = 0; i < sensorsPerFloor; i++){
                    accessPoints.add(new GeneratedAccessPoint(createName(), floor.key));
                }
            } else {
                // Just create a slightly different sensor-config for the special floor...
                for(int i = 0; i < sensorsPerFloor/2; i++){
                    accessPoints.add(new GeneratedAccessPoint(createName(), floor.key));
                }
            }
        }

        Map<String, List<GeneratedAccessPoint>> combineCandidates = new HashMap<>();
        for(GeneratedAccessPoint AP : accessPoints){
            if(rng.nextInt(100) < COMBINE_CHANCE){
                addToMap(combineCandidates, AP);
            }
        }

        List<GeneratedCombination> combinedAccessPoints = new ArrayList<>();
        for(List<GeneratedAccessPoint> APsOnFloor: combineCandidates.values()){
            if(APsOnFloor.size() < 2) continue;

            for(int i = 0; i < APsOnFloor.size();){ // @NOTE: No increment step
                int extraEntriesToCombine = rng.nextInt(2)+1;
                assert extraEntriesToCombine == 1 || extraEntriesToCombine == 2;

                if(i + extraEntriesToCombine < APsOnFloor.size()){
                    List<GeneratedAccessPoint> combined = new ArrayList<>();
                    for(int j = i; j <= i+extraEntriesToCombine; j++){
                        combined.add(APsOnFloor.get(j));
                    }

                    combinedAccessPoints.add(new GeneratedCombination(combined));
                    i += 1 + extraEntriesToCombine; // Increment for the AP we were on, plus the extra entries we added.
                } else {
                    // We could potentially try again with a smaller number of APs to combine... but just skip it.
                    break;
                }
            }
        }

        List<GeneratedAccessPoint> ignoredAccessPoints = new ArrayList<>();
        Set<String> combinedAccessPointNames = new HashSet<>();
        for(GeneratedCombination combination : combinedAccessPoints){
            for(GeneratedAccessPoint accessPoint : combination.combinedAccessPoints){
                combinedAccessPointNames.add(accessPoint.name);
            }
        }
        for(GeneratedAccessPoint accessPoint : accessPoints){
            // Just pick out some random access points to add to the ignore list. However, the parser in the benchmark
            //   doesn't allow ignored access points to be a part of a combination, so skip those...
            if(rng.nextInt(100) < IGNORE_CHANCE && !combinedAccessPointNames.contains(accessPoint.name)){
                ignoredAccessPoints.add(accessPoint);
            }
        }

        return new GeneratedMetadata(floors, accessPoints, combinedAccessPoints, ignoredAccessPoints);
    }

    private static void addToMap(Map<String, List<GeneratedAccessPoint>> map, GeneratedAccessPoint AP){
        if(!map.containsKey(AP.floorKey)){
            map.put(AP.floorKey, new ArrayList<>());
        }
        map.get(AP.floorKey).add(AP);
    }
}
