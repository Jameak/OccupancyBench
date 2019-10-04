package Benchmark.Generator;

import java.util.*;

public class Generator {
    public static Floor[] Generate(double scale, Random rng){
        assert scale > 0.0;
        int numberOfFloors = (int)Math.ceil(5 * scale);
        Floor[] floors = new Floor[numberOfFloors];

        floors[0] = FloorGenerator.GenerateGroundlevelFloor(rng);

        // Dont generate floor 0 because we just did that (ground floor)
        // Skip generating floor 1 because it's weird
        for(int i = 2; i < numberOfFloors+1; i++){
            floors[i-1] = FloorGenerator.GenerateFloor(i, rng);
            FloorGenerator.CreateCrossFloorJumps(floors[i-2], floors[i-1]);
        }

        return floors;
    }

    public static void AssignFloorsToIDs(Floor[] floors, MapData data, boolean preserveFloorAssociations){
        MapData.IdMap idMap = data.getIdMap();
        Map<AccessPoint.APLocation, Integer[]> locationMap = idMap.getLocationMap();

        Map<AccessPoint.APLocation, Set<Integer>> assignedIDs = new HashMap<>();

        for (Floor floor : floors) {
            AccessPoint[] floorAPs = floor.getAPs();

            int matchesFailedInARow = 0;
            for (int i = 0; i < floorAPs.length; i++) {
                AccessPoint AP = floorAPs[i];
                if (AP.hasMapID()) continue;
                Integer[] candidates = locationMap.get(AP.getLocation());
                boolean foundMatch = AssignIDToAP(idMap, assignedIDs, AP, candidates, floor.getFloorNumber(), preserveFloorAssociations);

                if (foundMatch) {
                    matchesFailedInARow = 0;
                } else {
                    matchesFailedInARow++;
                    if (matchesFailedInARow >= 2) {
                        throw new RuntimeException("Failed matching AP with ID after assignment reset. Aborting"); // TODO: Replace with something that can be caught at a higher level.
                    }

                    assert assignedIDs.get(AP.getLocation()) != null : "Failed to find match for AP for which no match can ever exist. Must be caused by mismatch between floor-generation and data-loading";
                    // We exhausted all valid matches from the given location in the source data, so reset them and try again.
                    // Only going to happen if the generated floorplan is bigger than the source data
                    assignedIDs.get(AP.getLocation()).clear();
                    i--; // Repeat this iteration of the loop
                }
            }
        }
    }

    public static void PrepareDataForGeneration(Floor[] generatedFloors, MapData data){
        Map<Integer, Integer> assignedAPidsAndAmountOfTimesAssigned = new HashMap<>();
        for(Floor floor : generatedFloors){
            for(AccessPoint AP : floor.getAPs()){
                AddToCount(assignedAPidsAndAmountOfTimesAssigned, AP.getMapID());
            }
        }

        for(Entry[] entriesAtSpecificTime : data.getDateEntries().values()){
            // Sum up all the probabilities of used APs at a specific timestamp entry.
            for(Entry entry : entriesAtSpecificTime){
                if(!entry.hasData()) continue;
                double totalAssignedProbability = 0.0;

                for(Integer APid : entry.getProbabilities().keySet()){
                    if(assignedAPidsAndAmountOfTimesAssigned.containsKey(APid)){
                        double probability = entry.getProbabilities().get(APid);

                        // If this AP has been combined with another AP then we should include that APs probability in the total
                        if(data.getIdMap().getCombineAPs().containsKey(APid)){
                            for(Integer otherAP : data.getIdMap().getCombineAPs().get(APid)){
                                if(entry.getProbabilities().containsKey(otherAP)){ // All combined APs _should_ always be present at any entry with my data, but it's not a requirement...
                                    probability += entry.getProbabilities().get(otherAP);
                                }
                            }
                            // Update the probability of this AP with the combined probability of all the APs that it's combined with.
                            // This allows us to ignore combined APs later on.
                            entry.getProbabilities().put(APid, probability);
                        }

                        int count = assignedAPidsAndAmountOfTimesAssigned.get(APid);
                        totalAssignedProbability += probability * count;
                    }
                }

                // Update probabilities of each used AP at the entry by dividing with the total.
                // This normalizes the total probability to 1 (100%).
                for(Integer APid : entry.getProbabilities().keySet()){
                    if(assignedAPidsAndAmountOfTimesAssigned.containsKey(APid)){
                        double probability = entry.getProbabilities().get(APid);
                        double newprobability = probability / totalAssignedProbability;
                        entry.getProbabilities().put(APid, newprobability);
                    }
                }
            }
        }

        // The probability of combined APs have been summed up into a single AP for every loaded entry, so we can disregard them from here on out.
        data.getIdMap().getCombineAPs().clear();
    }

    private static void AddToCount(Map<Integer, Integer> count, int APid){
        if(!count.containsKey(APid)){
            count.put(APid, 0);
        }

        count.put(APid, count.get(APid) + 1);
    }

    private static boolean AssignIDToAP(MapData.IdMap idMap, Map<AccessPoint.APLocation, Set<Integer>> assignedIDs, AccessPoint AP, Integer[] candidates, int APfloorNumber, boolean preserveFloors){
        for (Integer candidate : candidates) {
            if (IsAssigned(assignedIDs, AP.getLocation(), candidate)) continue;

            if(preserveFloors){
                assert APfloorNumber != 1 : "AP on floor 1 exists when floor 1 is skipped. Wtf";
                assert idMap.getFloorMap().keySet().size() > 1 : "Loaded data expected to contain at least 2 floors. Ground floor + a 'normal' floor";

                if(APfloorNumber == 0){
                    assert idMap.getFloorMap().get(APfloorNumber) != null : "Loaded data was expected to contain ground floor";
                    if(!idMap.getFloorMap().get(APfloorNumber).contains(candidate)) continue;
                } else {
                    int numFloorsInLoadedData = idMap.getFloorMap().keySet().size();
                    // Example of floor to check mapping:
                    //   Loaded data contains floors 0,2,3
                    //   Generated floors are: 0,2,3,4,5,6
                    //   FloorToCheck is: 2 -> 2, 3 -> 3, 4 -> 2, 5 -> 3, 6 -> 2, 7 -> 3
                    //   ... because we dont want to have more than 1 ground-floor.
                    int floorToCheck = ((APfloorNumber - 2) % (numFloorsInLoadedData - 1)) + 2;
                    assert idMap.getFloorMap().get(floorToCheck) != null : "Loaded data was expected to contain info about floor " + floorToCheck;
                    if(!idMap.getFloorMap().get(floorToCheck).contains(candidate)) continue;
                }
            }

            Integer[] mapPartners = idMap.getPartnerMap().get(candidate);
            if(mapPartners == null || mapPartners.length == 0){
                //Candidate doesn't have partners but AP does, so no match.
                if(AP.hasPartners()) continue;

                AP.setMapID(candidate);
                Assign(assignedIDs, AP.getLocation(), candidate);
                return true;
            } else if (AP.hasPartners() && mapPartners.length == AP.getPartners().length) {
                AP.setMapID(candidate);
                Assign(assignedIDs, AP.getLocation(), candidate);
                for (int k = 0; k < mapPartners.length; k++) {
                    assert !(AP.getPartners()[k].hasMapID()) : "AP has pre-assigned partners but is not assigned itself. Wtf?";
                    AP.getPartners()[k].setMapID(mapPartners[k]);
                    Assign(assignedIDs, AP.getPartners()[k].getLocation(), mapPartners[k]);
                }
                return true;
            }
        }

        return false;
    }

    private static boolean IsAssigned(Map<AccessPoint.APLocation, Set<Integer>> map, AccessPoint.APLocation location, Integer id){
        if(!map.containsKey(location)) return false;
        return map.get(location).contains(id);
    }

    private static void Assign(Map<AccessPoint.APLocation, Set<Integer>> map, AccessPoint.APLocation location, Integer id){
        if(!map.containsKey(location)){
            map.put(location, new HashSet<>());
        }

        map.get(location).add(id);
    }
}


