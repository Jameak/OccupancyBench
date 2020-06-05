package Benchmark.SeedLoader.Metadata;


import Benchmark.Logger;

import java.io.*;
import java.util.*;

public class MetadataLoader {
    // You can change this value if any of your sensor-names starts with this character...
    // but that is unlikely so it's not made into an actual config option
    private static final String COMMENT_IDENTIFIER = "#";

    public static FloorMetadata[] CombineMetadata(List<FloorData> floorMeta, Map<String, List<String>> floorSensorMapping,
                                                  Set<String> ignoreList, List<Set<String>> combinedList){
        Map<String, FloorMetadata> outFloorData = new HashMap<>();
        Map<String, String> accessPointToFloorMap = new HashMap<>();

        for(FloorData singleFloor : floorMeta){
            String floorKey = singleFloor.floorKey;
            if(!floorSensorMapping.containsKey(floorKey)){
                throw new IllegalStateException("Loaded floor to access point file missing data for floor with key " + floorKey);
            }

            List<String> APsOnFloor = floorSensorMapping.get(floorKey);
            List<String> APs = new ArrayList<>();
            for(String AP : APsOnFloor){
                if(ignoreList.contains(AP)){
                    Logger.LOG("INFO: MetadataLoader: AP with name " + AP + " in ignore-list was also present in the floor <-> access point mapping. It will be ignored.");
                    continue;
                }

                accessPointToFloorMap.put(AP, floorKey);
                APs.add(AP);
            }
            FloorMetadata finalFloor = new FloorMetadata(singleFloor.floorKey, singleFloor.eligibleForAutoScaling, APs);
            outFloorData.put(floorKey, finalFloor);
        }

        //Ensure that all combined APs are on the same floor, and then combine them by removing all but one of them from
        // the list of APs on the floor.
        for(Set<String> combinedAPs : combinedList){
            String correctFloorKey = null;
            String firstAPname = null;
            boolean first = true;
            for(String APname : combinedAPs){
                if(!accessPointToFloorMap.containsKey(APname)){
                    throw new IllegalStateException("Access point " + APname + " listed in combine-file not present in the floor <- access point mapping.");
                }

                String APfloor = accessPointToFloorMap.get(APname);
                if(correctFloorKey == null){
                    correctFloorKey = APfloor;
                    firstAPname = APname;
                } else {
                    if(!correctFloorKey.equals(APfloor)){
                        throw new IllegalStateException("Access point " + APname + " should be combined with " + firstAPname + " but is mapped to a different floor.");
                    }
                }

                // Remove all but the first AP from that floors list of APs.
                if(first){
                    first = false;
                } else {
                    outFloorData.get(APfloor).accessPointsOnFloor.remove(APname);
                    outFloorData.get(APfloor).combinedAccessPoints.put(APname, firstAPname);
                }
            }
        }

        return outFloorData.values().toArray(new FloorMetadata[0]);
    }

    public static List<FloorData> LoadFloorFile(final String floorFile, final String separator) throws IOException {
        final List<FloorData> data = new ArrayList<>();

        Parse(floorFile, line -> {
            String[] parts = line.split(separator);
            assert parts.length == 2 :
                    "Line has " + parts.length + " but expected 2. Configured separator is " + separator + ".\n" +
                    "    File: " + floorFile + "\n    Line: " + line;
            String floorKey = parts[0];
            assert parts[1].equalsIgnoreCase("true") || parts[1].equalsIgnoreCase("false") :
                    "Value " + parts[1] + " will always evaluate to false. Use values 'true' or 'false'.\n" +
                    "    File: " + floorFile + "\n    Line: " + line;
            boolean autoScalingEligibility = Boolean.parseBoolean(parts[1]);
            data.add(new FloorData(floorKey, autoScalingEligibility));
        });

        return data;
    }

    public static Map<String, List<String>> LoadFloorToAccessPointFile(final String floorToAccessPointFile, final String separator) throws IOException{
        final Map<String, List<String>> data = new HashMap<>();

        Parse(floorToAccessPointFile, line -> {
            String[] parts = line.split(separator);
            assert parts.length == 2 : "Line has " + parts.length + " but expected 2. Configured separator is " + separator + ".\n" +
                    "    File: " + floorToAccessPointFile + "\n    Line: " + line;
            String floorKey = parts[0];
            String accessPointName = parts[1];

            if(!data.containsKey(floorKey)){
                data.put(floorKey, new ArrayList<>());
            }
            data.get(floorKey).add(accessPointName);
        });

        return data;
    }

    public static Set<String> LoadIgnoreFile(final String ignoreFile) throws IOException {
        final Set<String> data = new HashSet<>();
        Parse(ignoreFile, data::add);
        return data;
    }

    public static List<Set<String>> LoadCombinedFile(final String combinedFile, final String separator) throws IOException {
        final List<Set<String>> data = new ArrayList<>();
        Parse(combinedFile, line -> {
            Set<String> sensorsOnLine = new HashSet<>(Arrays.asList(line.split(separator)));
            // Lines with only 1 AP dont really make sense, so just skip them because nothing needs to be done.
            if(sensorsOnLine.size() != 1){
                data.add(sensorsOnLine);
            }
        });

        // Check for APs appearing on multiple lines in the file.
        // We dont want to allow cycles caused by file contents such as:
        //   AP-1234,AP-ABCD
        //   AP-ABCD,AP-1234
        Set<String> allSensorsInFile = new HashSet<>();
        for(Set<String> sensorsOnLine : data){
            for(String APname : sensorsOnLine){
                if(allSensorsInFile.contains(APname)){
                    throw new IllegalStateException("Combined file has the same AP appear on multiple lines. Not allowed.");
                } else {
                    allSensorsInFile.add(APname);
                }
            }
        }

        return data;
    }

    public static Map<String, String> LoadIdMap(final String idMapFile, final String separator) throws IOException {
        final Map<String,String> data = new HashMap<>();
        Parse(idMapFile, line -> {
            String[] parts = line.split(separator);
            assert parts.length == 2 : "Line has " + parts.length + " but expected 2. Configured separator is " + separator + ".\n" +
                    "    File: " + idMapFile + "\n    Line: " + line;
            String key = parts[0];
            String APname = parts[1];
            data.put(key,APname);
        });
        return data;
    }

    private static void Parse(String file, LineHandler lineHandler) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            String line;
            while((line = reader.readLine()) != null){
                line = line.trim();
                if(isCommentLine(line) || isBlankLine(line)) continue;
                line = stripCommentIfPresent(line);
                lineHandler.Handle(line);
            }
        }
    }

    private interface LineHandler{
        void Handle(String line);
    }

    private static boolean isCommentLine(String line){
        return line.startsWith(COMMENT_IDENTIFIER);
    }

    private static boolean isBlankLine(String line){
        return line.length() == 0;
    }

    private static String stripCommentIfPresent(String line) {
        if(line.contains(COMMENT_IDENTIFIER)){
            line = line.substring(0, line.indexOf(COMMENT_IDENTIFIER));
            line = line.trim();
        }
        return line;
    }

    public static class FloorData {
        public final String floorKey;
        public final boolean eligibleForAutoScaling;

        public FloorData(String floorKey, boolean eligibleForAutoScaling){
            this.floorKey = floorKey;
            this.eligibleForAutoScaling = eligibleForAutoScaling;
        }
    }
}
