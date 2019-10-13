package Benchmark.Loader;

import Benchmark.Generator.GeneratedData.AccessPoint;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;

/**
 * Top-level class of data loaded from the source data.
 */
public class MapData {
    private final IdMap idMap;
    private final Map<LocalDate, Entry[]> loadedEntries;

    public MapData(IdMap idMap, Map<LocalDate, Entry[]> loadedDateEntries){
        this.idMap = idMap;
        this.loadedEntries = loadedDateEntries;
    }

    public Map<LocalDate, Entry[]> getDateEntries() {
        return loadedEntries;
    }

    public IdMap getIdMap() {
        return idMap;
    }

    public static class IdMap{
        private final Map<AccessPoint.APLocation, Integer[]> locationMap;
        private final Map<Integer, Integer[]> partnerMap;
        private final Map<Integer, Integer[]> combinedAPs;
        private final Map<Integer, Set<Integer>> floorMap;

        public IdMap(Map<AccessPoint.APLocation, Integer[]> locationMap, Map<Integer, Integer[]> partnerMap, Map<Integer, Integer[]> combineAPs, Map<Integer, Set<Integer>> floorMap){
            this.locationMap = locationMap;
            this.partnerMap = partnerMap;
            this.combinedAPs = combineAPs;
            this.floorMap = floorMap;
        }

        public Map<Integer, Integer[]> getPartnerMap() {
            return partnerMap;
        }

        public Map<AccessPoint.APLocation, Integer[]> getLocationMap() {
            return locationMap;
        }

        public Map<Integer, Integer[]> getCombineAPs() {
            return combinedAPs;
        }

        public Map<Integer, Set<Integer>> getFloorMap() {
            return floorMap;
        }
    }
}
