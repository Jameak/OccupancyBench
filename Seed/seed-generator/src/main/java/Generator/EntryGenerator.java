package Generator;

import Generator.Generated.GeneratedAccessPoint;
import Generator.Generated.GeneratedEntry;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

public class EntryGenerator {
    private final int DOWNTIME_INDEX_START = 16; // Valid range is 0 <= DOWNTIME_INDEX_START < DOWNTIME_INDEX_END
    private final int DOWNTIME_INDEX_END   = 20; // Valid range is DOWNTIME_INDEX_START < DOWNTIME_INDEX_END < EntryPatterns.PATTERN_LENGTH

    // Include start/end indexes for when to apply jitter here mainly just so that we dont generate large amounts of connected clients at night
    private final int JITTER_INDEX_START = 30; // Valid range is 0 <= JITTER_INDEX_START < JITTER_INDEX_END
    private final int JITTER_INDEX_END   = 70; // Valid range is JITTER_INDEX_START < JITTER_INDEX_END < EntryPatterns.PATTERN_LENGTH
    private final int JITTER_MAX = 4; // Must be > 0

    private final int sampleRate;
    private final int sensorDowntimeChance;
    private final boolean includeSystemDowntime;
    private final boolean includeJitter;
    private final boolean createIdMap;
    private final double peakScale;
    private final Random rng;
    private final Map<String,String> nameToId = new HashMap<>();
    private final Map<String,String> idToName = new HashMap<>();
    private int id = 0;
    private final EntryPatterns entryPatterns;

    public EntryGenerator(int sampleRate, int sensorDowntimeChance, boolean includeSystemDowntime, boolean includeJitter, boolean createIdMap, double peakScale, int rngSeed){
        this.sampleRate = sampleRate;
        this.sensorDowntimeChance = sensorDowntimeChance;
        this.includeSystemDowntime = includeSystemDowntime;
        this.includeJitter = includeJitter;
        this.createIdMap = createIdMap;
        this.peakScale = peakScale;
        this.rng = new Random(rngSeed);
        this.entryPatterns = new EntryPatterns(sampleRate);
    }

    /**
     * Given a list of access points, create a pattern for each access point for use during entry-generation.
     */
    public List<AssignedDataSource> assignDataSources(List<GeneratedAccessPoint> accessPoints){
        if(createIdMap){
            // The shortened name could easily be a string, but sequential numbers are simpler to generate and compress better.
            for(GeneratedAccessPoint accessPoint : accessPoints){
                nameToId.put(accessPoint.name, "" + id);
                idToName.put("" + id, accessPoint.name);
                id++;
            }
        } else {
            for(GeneratedAccessPoint accessPoint : accessPoints){
                // Not strictly needed. Just simplifies the rest of the generator-implementation.
                nameToId.put(accessPoint.name, accessPoint.name);
            }
        }

        List<AssignedDataSource> sourceData = new ArrayList<>();
        for(GeneratedAccessPoint accessPoint : accessPoints){
            int[] pattern = EntryPatterns.getRandomPattern(rng);

            for(int i = 0; i < pattern.length; i++){
                pattern[i] = (int) Math.floor(pattern[i] * peakScale);
            }

            if(includeJitter){
                for(int i = JITTER_INDEX_START; i < JITTER_INDEX_END; i++){
                    pattern[i] = pattern[i] + (rng.nextInt(JITTER_MAX) - JITTER_MAX/2);
                }
            }

            if(includeSystemDowntime){
                for(int i = DOWNTIME_INDEX_START; i < DOWNTIME_INDEX_END; i++){
                    pattern[i] = 0;
                }
            }

            for(int i = 0; i < pattern.length; i++){
                if(pattern[i] < 0) pattern[i] = 0;
            }

            sourceData.add(new AssignedDataSource(accessPoint, pattern));
        }

        return sourceData;
    }

    public Map<String,String> getIdMap(){
        if(createIdMap) return idToName;
        else return new HashMap<>();
    }

    /**
     * Generate entries for the given data using the given data-sources (access points + patterns)
     */
    public List<GeneratedEntry> generateEntries(LocalDate date, List<AssignedDataSource> dataSources){
        List<GeneratedEntry> entries = new ArrayList<>();

        LocalDateTime time = LocalDateTime.of(date, LocalTime.of(0,0));
        LocalDateTime endTime = time.plusDays(1);
        int entriesSinceLastPairing = 0;
        EntryPatterns.TimeIndexPairing[] pairings = null;
        while(time.isBefore(endTime)){
            Map<String, Integer> initialSensorEntries = new HashMap<>(); // Has direct sensor-values.
            Map<String, Double> finalSensorEntries = new HashMap<>(); // Has sensor-probabilities instead.

            if(entriesSinceLastPairing == 0){
                pairings = entryPatterns.getPairing(time.toLocalTime());
            }

            int totalClients = 0;
            for(AssignedDataSource dataSource : dataSources){
                int value = entryPatterns.getValueForTime(pairings, dataSource.pattern, entriesSinceLastPairing);

                // Model full system downtime by not writing any data.
                if (includeSystemDowntime && pairings[0].index >= DOWNTIME_INDEX_START && pairings[1].index < DOWNTIME_INDEX_END) {
                    continue;
                }

                // Small chance that the sensor isn't reachable.
                // For single-sensor downtime, make that sensor absent from that entry-list.
                if(rng.nextInt(100) < sensorDowntimeChance){
                    continue;
                }

                if(includeJitter && pairings[0].index > JITTER_INDEX_START && pairings[0].index < JITTER_INDEX_END){
                    value = value + (rng.nextInt(JITTER_MAX) - JITTER_MAX/2);
                    if(value < 0) value = 0;
                }

                assert value >= 0;
                initialSensorEntries.put(nameToId.get(dataSource.AP.name), value);
                totalClients += value;
            }

            for(Map.Entry<String, Integer> sensorEntry : initialSensorEntries.entrySet()){
                finalSensorEntries.put(sensorEntry.getKey(), totalClients == 0 ? 0.0 : ((double)sensorEntry.getValue()) / totalClients);
            }

            entries.add(new GeneratedEntry(time, totalClients, finalSensorEntries));
            time = time.plusSeconds(sampleRate);

            // Are we still within range of our pairing-timestamps?
            if(time.toLocalTime().isAfter(pairings[0].time) && time.toLocalTime().isBefore(pairings[0].time)){
                entriesSinceLastPairing++;
            } // Special-case handling for the interval of 23:45 to 00:00 because the ordering of LocalTime says that 00:00 < 23:45
            else if (pairings[0].time.equals(LocalTime.of(23, 45))) {

                // We had an exact time-match on pairings.
                if(time.toLocalTime().isAfter(pairings[0].time) &&
                        time.toLocalTime().isAfter(pairings[1].time) &&
                        !pairings[1].time.equals(LocalTime.MIDNIGHT)) {
                    assert pairings[0].time.equals(pairings[1].time) :
                            "pairings[0]:" + pairings[0].time + " " + "pairings[1]:" + pairings[1].time;
                    // We exited the range of the pairing-timestamps that we found.
                    // Next loop iteration must update pairing info.
                    entriesSinceLastPairing = 0;
                } else {
                    // Special-case handling for the interval of 23:45 to 00:00 because the ordering of LocalTime says that 00:00 < 23:45
                    assert pairings[1].time.equals(LocalTime.MIDNIGHT) : "pairings[1]:" + pairings[1].time;
                    entriesSinceLastPairing++;
                }
            } else {
                // We exited the range of the pairing-timestamps that we found.
                // Next loop iteration must update pairing info.
                entriesSinceLastPairing = 0;
            }
        }

        return entries;
    }

    public static class AssignedDataSource {
        public final GeneratedAccessPoint AP;
        public final int[] pattern;

        public AssignedDataSource(GeneratedAccessPoint AP, int[] pattern){
            assert pattern.length == EntryPatterns.PATTERN_LENGTH;
            this.AP = AP;
            this.pattern = pattern;
        }
    }
}
