package Benchmark.Loader;

import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Logger;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Class responsible for parsing the source data into something usable for combining with the generated data.
 * The input-files containing source-data is in a csv-ish format. See the python-script that generates the
 * files for file-format details.
 *
 * This class contains large switch-statements responsible for mapping the access points in the source data
 * with metadata about what floor the access point is on, where it is located, what access-points they're
 * combined with, etc.
 */
public class MapParser {
    public static MapData ParseMap(String idMapPath, String directoryWithProbabilityData) throws IOException{
        File idMapFile = new File(idMapPath);
        MapData.IdMap parsedIdMap;
        try {
            parsedIdMap = ParseIdMap(idMapFile);
        } catch (IOException e) {
            Logger.LOG("IO Error while attempting to read Id Map file.");
            throw e;
        }

        Map<LocalDate, Entry[]> loadedEntries = new HashMap<>();
        File dir = new File(directoryWithProbabilityData);
        File[] filesToParse = dir.listFiles();
        if(filesToParse == null){
            Logger.LOG("No files in directory");
            throw new IOException("No files in directory");
        } else {
            try {
                for(File file : filesToParse){
                    // Skip the IdMap file in case it's in the same directory as the probability data. TODO: Very brittle comparison. Should be improved.
                    if(file.compareTo(idMapFile) == 0){
                        continue;
                    }

                    Entry[] entries = ParseProbabilityMap(file);

                    assert entries.length > 0;
                    // Grab the first date-entry in the parsed file.
                    LocalDate date = entries[0].getTime().toLocalDate();
                    loadedEntries.put(date, entries);
                }
            } catch (IOException e) {
                Logger.LOG("IO Error while attempting to read probability map file.");
                throw e;
            }
        }

        return new MapData(parsedIdMap, loadedEntries);
    }

    private static Entry[] ParseProbabilityMap(File mapFile) throws IOException {
        List<Entry> parsedEntries = new ArrayList<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(mapFile))){
            String time = null;
            int total = -1;
            Map<Integer, Double> probabilities = new HashMap<>();

            String line;
            while((line = reader.readLine()) != null){
                String[] entries = line.split(";");
                assert entries.length < 3;

                switch (entries[0]) {
                    case "Time":
                        if (time != null) {
                            parsedEntries.add(new Entry(LocalDateTime.parse(time.replace("Z", "")), total, probabilities));
                            total = -1;
                            probabilities = new HashMap<>();
                        }
                        time = entries[1];
                        break;
                    case "Total clients":
                        total = Integer.parseInt(entries[1]);
                        break;
                    case "NO DATA":
                        continue;
                    default:
                        Integer APid = Integer.parseInt(entries[0]);
                        Double probability = Double.parseDouble(entries[1]);
                        probabilities.put(APid, probability);
                        break;
                }
            }

            if(time != null){
                parsedEntries.add(new Entry(LocalDateTime.parse(time.replace("Z", "")), total, probabilities));
            }
        }

        return parsedEntries.toArray(new Entry[0]);
    }

    private static MapData.IdMap ParseIdMap(File idFile) throws IOException {
        // Mapping from APLocation-type -> AP-ids, to facilitate randomly assigning APs from the ITU data to generated
        // rooms of a specific type, so that we get an actually "random" occupancy-schedule rather than just a
        // straight copy of the ITU one.
        Map<AccessPoint.APLocation, List<Integer>> APtypeMap = new HashMap<>();
        // A mapping from partner-constant -> AP-ids, for APs that are located in the same room, and are therefore
        // likely to have high client-churn between them.
        Map<String, List<Integer>> unfinishedPartners = new HashMap<>();
        // A mapping from AP-id -> AP-ids, for combining several APs in the ITU dataset to a single AP in the generator.
        Map<String, List<Integer>> unfinishedCombinedAPs = new HashMap<>();
        // A mapping from floor-number to AP-ids on that floor.
        Map<Integer, Set<Integer>> floorMap = new HashMap<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(idFile))){
            String line;
            while((line = reader.readLine()) != null) {
                String[] entries = line.split(";");
                assert entries.length == 2;
                Integer APid = Integer.parseInt(entries[0]);
                String APname = entries[1];

                FloorMapSwitch(floorMap, APname, APid);

                REDACTED DATA
            }
        }

        // Finish up the combined information
        Map<Integer, Integer[]> combinedAPs = new HashMap<>();
        Set<Integer> ignoredIds = new HashSet<>();
        for(String combinedConstant : unfinishedCombinedAPs.keySet()){
            List<Integer> combined = unfinishedCombinedAPs.get(combinedConstant);
            int length = combined.size();
            assert length > 0;
            if(length == 1){
                System.out.println("AP expected to be computed with at least 1 other AP, but has none: " + combined);
                System.out.println("This is likely caused by the start- and end-date of the input data not containing the expected APs.");
                System.out.println("Combination will be ignored and generation will continue. Asserts elsewhere in the program may fail due to this");
                ignoredIds.add(combined.get(0));
                continue;
            }

            List<Integer> otherIDs = new ArrayList<>(length);
            for(int i = 1; i < length; i++){
                otherIDs.add(combined.get(i));
                // Remove all combined IDs except for the first one from the final list of available APs so that combined APs dont get assigned to different locations.
                ignoredIds.add(combined.get(i));
            }

            combinedAPs.put(combined.get(0), otherIDs.toArray(new Integer[0]));
        }

        // Finish up the partner information
        Map<Integer, Integer[]> partnerMap = new HashMap<>();
        for(String partnerConstant : unfinishedPartners.keySet()){
            List<Integer> partners = unfinishedPartners.get(partnerConstant);
            int length = partners.size();
            if(length < 2){
                throw new IllegalStateException("AP expected to have at least 1 partner, but has none: " + partners);
            }

            for(int i = 0; i < length; i++){
                List<Integer> otherIDs = new ArrayList<>(length);
                for(int j = 0; j < length; j++){
                    if(i == j) continue;
                    otherIDs.add(partners.get(j));
                }
                partnerMap.put(partners.get(i), otherIDs.toArray(new Integer[0]));
            }
        }

        Map<AccessPoint.APLocation, Integer[]> APtypeMapFinished = new HashMap<>();
        for(AccessPoint.APLocation location : APtypeMap.keySet()){
            List<Integer> APsAtLocation = APtypeMap.get(location);
            List<Integer> cleanAPs = new ArrayList<>(APsAtLocation.size());
            for(Integer APid : APsAtLocation){
                if(ignoredIds.contains(APid)) {
                    // If we want to ignore the ID, then remove it from the floor map
                    for(Set<Integer> floorAPs : floorMap.values()){
                        //The 'contains' call is included for clarity, even though it can be omitted.
                        //noinspection RedundantCollectionOperation
                        if(floorAPs.contains(APid)) floorAPs.remove(APid);
                    }
                } else {
                    //Exclude APs that have been ignored because they're combined with another AP.
                    cleanAPs.add(APid);
                }
            }
            APtypeMapFinished.put(location, cleanAPs.toArray(new Integer[0]));
        }

        return new MapData.IdMap(APtypeMapFinished, partnerMap, combinedAPs, floorMap);
    }

    private static void AddToFloorSet(Map<Integer, Set<Integer>> map, int floor, int id){
        if(!map.containsKey(floor)){
            map.put(floor, new HashSet<>());
        }

        map.get(floor).add(id);
    }

    private static <T> void AddToMap(Map<T, List<Integer>> map, T constant, int id){
        if(!map.containsKey(constant)){
            map.put(constant, new ArrayList<>());
        }

        map.get(constant).add(id);
    }

    private static void FloorMapSwitch(Map<Integer, Set<Integer>> map, String APname, int APid) {
        REDACTED DATA
    }
}
