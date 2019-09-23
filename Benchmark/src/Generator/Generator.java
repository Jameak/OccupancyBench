package Generator;

import java.util.*;

public class Generator {
    public static Floor[] Generate(double scale, Random rng){
        assert scale > 0.0;
        int numberOfFloors = (int)Math.ceil(5 * scale);
        Floor[] floors = new Floor[numberOfFloors+1];

        floors[0] = FloorGenerator.GenerateGroundlevelFloor(rng);

        for(int i = 1; i < numberOfFloors+1; i++){
            floors[i] = FloorGenerator.GenerateFloor(i, rng);
            FloorGenerator.CreateCrossFloorJumps(floors[i-1], floors[i]);
        }

        return floors;
    }

    public static void AssignFloorsToIDs(Floor[] floors, MapData data){
        MapData.IdMap idMap = data.getIdMap();
        Map<AccessPoint.APLocation, Integer[]> locationMap = idMap.getLocationMap();

        Map<Generator.AccessPoint.APLocation, Set<Integer>> assignedIDs = new HashMap<>();

        for (Floor floor : floors) {
            AccessPoint[] floorAPs = floor.getAPs();

            int matchesFailedInARow = 0;
            for (int i = 0; i < floorAPs.length; i++) {
                AccessPoint AP = floorAPs[i];
                if (AP.hasMapID()) continue;
                Integer[] candidates = locationMap.get(AP.getLocation());
                boolean foundMatch = AssignIDToAP(idMap, assignedIDs, AP, candidates);

                if (foundMatch) {
                    matchesFailedInARow = 0;
                } else {
                    matchesFailedInARow++;
                    if (matchesFailedInARow >= 2) {
                        throw new RuntimeException("Failed matching AP with ID after assignment reset. Aborting"); // TODO: Replace with something that can be caught at a higher level.
                    }

                    // We exhausted all valid matches from the given location in the source data, so reset them and try again.
                    // Only going to happen if the generated floorplan is bigger than the source data
                    assignedIDs.get(AP.getLocation()).clear();
                    i--; // Repeat this iteration of the loop
                }
            }
        }
    }

    private static boolean AssignIDToAP(MapData.IdMap idMap, Map<Generator.AccessPoint.APLocation, Set<Integer>> assignedIDs, AccessPoint AP, Integer[] candidates){
        for (Integer candidate : candidates) {
            if (IsAssigned(assignedIDs, AP.getLocation(), candidate)) continue;

            Integer[] mapPartners = idMap.getPartnerMap().get(candidate);
            if (mapPartners.length == AP.getPartners().length) {
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

    private static boolean IsAssigned(Map<Generator.AccessPoint.APLocation, Set<Integer>> map, Generator.AccessPoint.APLocation location, Integer id){
        if(!map.containsKey(location)) return false;
        return map.get(location).contains(id);
    }

    private static void Assign(Map<Generator.AccessPoint.APLocation, Set<Integer>> map, Generator.AccessPoint.APLocation location, Integer id){
        if(!map.containsKey(location)){
            map.put(location, new HashSet<>());
        }

        map.get(location).add(id);
    }
}


