package Benchmark.SeedLoader.Seeddata;

import Benchmark.SeedLoader.Metadata.FloorMetadata;
import Benchmark.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Class responsible for parsing the source data into something usable for combining with the generated data.
 * The input-files containing source-data is in a csv-ish format. See the python-script that generates the
 * files for file-format details.
 */
public class SeeddataLoader {

    public static SeedEntries LoadSeedData(String separator, String directoryWithProbabilityData,
                                           Set<String> ignoreList, FloorMetadata[] floorMetadata) throws IOException {
        Set<String> allUnignoredAPs = getAllUnignoredAccessPoints(floorMetadata);
        Map<String,String> idMap = new HashMap<>();
        for(String AP : allUnignoredAPs){
            idMap.put(AP,AP);
        }

        return LoadSeedData(separator, directoryWithProbabilityData, ignoreList, floorMetadata, idMap);
    }

    public static SeedEntries LoadSeedData(String separator, String directoryWithProbabilityData,
                                           Set<String> ignoreList, FloorMetadata[] floorMetadata,
                                           Map<String, String> idMap) throws IOException {
        Set<String> allUnignoredAPs = getAllUnignoredAccessPoints(floorMetadata);
        return LoadSeedData(separator, directoryWithProbabilityData, ignoreList, floorMetadata, idMap, allUnignoredAPs);
    }

    private static SeedEntries LoadSeedData(String separator, String directoryWithProbabilityData,
                                            Set<String> ignoreList, FloorMetadata[] floorMetadata,
                                            Map<String, String> idMap, Set<String> allUnignoredAPs) throws IOException {
        Map<LocalDate, Entry[]> loadedEntries = new HashMap<>();
        File dir = new File(directoryWithProbabilityData);
        File[] filesToParse = dir.listFiles();
        if(filesToParse == null){
            Logger.LOG("No files in seed data directory");
            throw new IOException("No files in directory");
        } else {
            try {
                for(File file : filesToParse){
                    List<Entry> parsedEntries = ParseSeedFile(separator, file, ignoreList, idMap, allUnignoredAPs);
                    Entry[] entries = parsedEntries.toArray(new Entry[0]);

                    // Seed data file is empty.
                    if(entries.length == 0){
                        continue;
                    }

                    LocalDate firstDate = entries[0].getTime().toLocalDate();
                    for(int i = 1; i < entries.length; i++){
                        if(!entries[i].getTime().toLocalDate().equals(firstDate)){
                            // We could probably support this, but I dont need it so we just abort.
                            throw new IllegalStateException("Seed data file contains data for multiple dates.");
                        }
                    }

                    for(Entry entry : entries){
                        if(!entry.hasData()) continue;
                        CombineDataForAccessPoints(entry.getProbabilities(), floorMetadata);
                    }

                    if(loadedEntries.containsKey(firstDate)){
                        throw new IllegalStateException("Seed data file contains data for date that another file also contains data for. Combine all the data for a specific date in that file.");
                    }
                    // We index seed files by the date of their contents
                    loadedEntries.put(firstDate, entries);
                }
            } catch (IOException e) {
                Logger.LOG("IO Error while attempting to read probability map file.");
                throw e;
            }
        }

        return new SeedEntries(loadedEntries);
    }

    private static void CombineDataForAccessPoints(Map<String, Double> probabilities, FloorMetadata[] floorMetadata) {
        assert probabilities != null && !probabilities.isEmpty() : "Entry to combine data for has no data.";

        for(FloorMetadata floorData : floorMetadata){
            for(String APtoCombine : floorData.combinedAccessPoints.keySet()){
                if(probabilities.containsKey(APtoCombine)){
                    Double prop = probabilities.get(APtoCombine);
                    String targetAP = floorData.combinedAccessPoints.get(APtoCombine);
                    assert !APtoCombine.equals(targetAP) : "Seed data says to combine AP with itself... " +
                            "MetadataLoader should have complained during loading so this should never happen.";

                    if(probabilities.containsKey(targetAP)){
                        probabilities.put(targetAP, probabilities.get(targetAP) + prop);
                    } else {
                        probabilities.put(targetAP, prop);
                    }
                    //Remove the old data.
                    probabilities.remove(APtoCombine);
                }
            }
        }
    }

    private static List<Entry> ParseSeedFile(String separator, File entryFile, Set<String> ignoreList,
                                             Map<String, String> idMap, Set<String> allUnignoredAPs) throws IOException {
        List<Entry> parsedEntries = new ArrayList<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(entryFile))){
            String time = null;
            int total = -1;
            Map<String, Double> probabilities = new HashMap<>();

            String line;
            while((line = reader.readLine()) != null){
                String[] entries = line.split(separator);
                assert entries.length < 3 : "Too much data on line: " + line;

                switch (entries[0]) {
                    case "Time":
                        if (time != null) {
                            parsedEntries.add(new Entry(ParseDate(time), total, probabilities));
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
                        String seedAPName = entries[0];

                        // If the pre- or post-idmap translated name exists in the ignore-list, just skip it.
                        if(ignoreList.contains(seedAPName) || (idMap.containsKey(seedAPName) && ignoreList.contains(idMap.get(seedAPName)))){
                            continue;
                        }

                        if(!idMap.containsKey(seedAPName)){
                            // This isn't 100% required, we could just silently skip entries that we dont know anything about.
                            //   However, this forces the seed-metadata to contain all APs that exist in the data to ensure that
                            //   the user doesn't unknowingly ignore some of the data in their dataset. Here we force them
                            //   to add it to the ignore-list if they intended to ignore it.
                            throw new IllegalStateException("Seed data entry with name " + seedAPName + " not present in idmap. " +
                                    "If you provided a non-empty IdMap file, then please add this AP and its mapping there. " +
                                    "If you did not, then you probably forgot to add the AP to the floormap-file, the " +
                                    "combined-file or the ignore-file.");
                        }

                        String properAPName = idMap.get(seedAPName);
                        if(!allUnignoredAPs.contains(properAPName)){
                            // This isn't 100% required, we could just silently skip entries that we dont know anything about.
                            //   However, this forces the seed-metadata to contain all APs that exist in the data to ensure that
                            //   the user doesn't unknowingly ignore some of the data in their dataset.
                            throw new IllegalStateException("Seed data entry with name " + seedAPName + " translated to "
                                    + properAPName + " via the IdMap is not present in the floor metadata. " +
                                    "Did you forget to add it to the floormap-file or combined-file?");
                        }

                        Double probability = Double.parseDouble(entries[1]);
                        probabilities.put(properAPName, probability);
                        break;
                }
            }

            // This ensures we also add the final data-point in the seed data.
            if(time != null){
                parsedEntries.add(new Entry(ParseDate(time), total, probabilities));
            }
        }

        return parsedEntries;
    }

    private static Set<String> getAllUnignoredAccessPoints(FloorMetadata[] floorMetadata){
        Set<String> allAPs = new HashSet<>();
        for(FloorMetadata floor : floorMetadata){
            allAPs.addAll(floor.accessPointsOnFloor);
            allAPs.addAll(floor.combinedAccessPoints.keySet());
        }
        return allAPs;
    }

    private static LocalDateTime ParseDate(String time){
        return LocalDateTime.parse(time.replace("Z", ""));
    }
}
