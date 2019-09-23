package Generator;

import java.util.Map;

public class MapData {
    private final IdMap idMap;
    private final Map<String, Entry[]> loadedEntries;

    public MapData(IdMap idMap, Map<String, Entry[]> loadedEntries){
        this.idMap = idMap;
        this.loadedEntries = loadedEntries;
    }

    public Map<String, Entry[]> getLoadedEntries() {
        return loadedEntries;
    }

    public IdMap getIdMap() {
        return idMap;
    }

    public static class IdMap{
        private final Map<AccessPoint.APLocation, Integer[]> locationMap;
        private final Map<Integer, Integer[]> partnerMap;
        private final Map<Integer, Integer[]> combinedAPs;

        public IdMap(Map<AccessPoint.APLocation, Integer[]> locationMap, Map<Integer, Integer[]> partnerMap, Map<Integer, Integer[]> combineAPs){
            this.locationMap = locationMap;
            this.partnerMap = partnerMap;
            this.combinedAPs = combineAPs;
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
    }
}
