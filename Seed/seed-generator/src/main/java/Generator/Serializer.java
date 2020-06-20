package Generator;

import Generator.Generated.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Serializer {
    private static final String FLOOR_FILE_NAME = "floors.csv";
    private static final String FLOORMAP_FILE_NAME = "floormap.csv";
    private static final String IGNORED_FILE_NAME = "ignored.csv";
    private static final String COMBINED_FILE_NAME = "combined.csv";
    private static final String IDMAP_FILE_NAME = "idmap.csv";
    private static final String ENTRY_FOLDER_NAME = "entries";

    private final Path targetFolder;
    private final Path targetEntryFolder;
    private final String csvSeparator;

    public Serializer(Path outputFolder, String csvSeparator, boolean deleteExisting) throws IOException {
        this.csvSeparator = csvSeparator;
        assert outputFolder.toFile().exists();
        this.targetFolder = outputFolder;
        this.targetEntryFolder = targetFolder.resolve(ENTRY_FOLDER_NAME);

        if(targetEntryFolder.toFile().exists()){
            if(deleteExisting){
                deleteDirectory(targetEntryFolder);
                targetEntryFolder.toFile().mkdirs();
            } else {
                String[] filesInDir = targetEntryFolder.toFile().list();
                if(filesInDir != null && filesInDir.length > 0){
                    System.out.println("Warning: Output folder for entries (" + targetEntryFolder + ") not empty.\n" +
                                       "If the files in this folder are left-over from a previous run and aren't\n" +
                                       "overwritten this run, then the content of those other files may be outdated\n" +
                                       "and should be deleted.");
                }
            }
        } else {
            targetEntryFolder.toFile().mkdirs();
        }
    }

    private void deleteDirectory(Path path) throws IOException {
        if(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)){
            try (DirectoryStream<Path> values = Files.newDirectoryStream(path)) {
                for (Path value : values){
                    deleteDirectory(value);
                }
            }
        }
        Files.delete(path);
    }

    /**
     * Writes the metadata to disk in the specified target folder.
     */
    public void writeMetadata(GeneratedMetadata metadata) throws IOException {
        // Construct and write the 'floors' file.
        List<String> floorLines = new LinkedList<>();
        for(GeneratedFloor floor : metadata.floorData){
            String line = floor.key + csvSeparator + floor.eligibleForScaling;
            floorLines.add(line);
        }
        Files.write(targetFolder.resolve(FLOOR_FILE_NAME), floorLines, StandardCharsets.UTF_8);

        // Construct and write the 'floormap' file.
        List<String> floormapLines = new LinkedList<>();
        for(GeneratedAccessPoint sensor : metadata.accessPoints){
            // Don't write the ignored APs to the floormap file. The floormap file is allowed to contain ignored APs,
            //   but they'll just get ignored with an INFO-message from the benchmark when we parse the files. So we
            //   don't include them in that file here to avoid spamming the console during benchmark-parsing.
            // @NOTE: Our list of ignored access points should always be short enough that this check isn't a
            //        performance-issue... but should maybe replace this list with a set?
            if(metadata.ignoredAccessPoints.contains(sensor)){
                continue;
            }

            String line = sensor.floorKey + csvSeparator + sensor.name;
            floormapLines.add(line);
        }
        Files.write(targetFolder.resolve(FLOORMAP_FILE_NAME), floormapLines, StandardCharsets.UTF_8);

        // Construct and write the 'combined' file.
        List<String> combinedLines = new LinkedList<>();
        for(GeneratedCombination combined : metadata.combinations){
            StringBuilder line = new StringBuilder();
            boolean first = true;
            for(GeneratedAccessPoint accessPoint : combined.combinedAccessPoints){
                if(first){
                    line.append(accessPoint.name);
                    first = false;
                } else {
                    line.append(csvSeparator).append(accessPoint.name);
                }
            }
            combinedLines.add(line.toString());
        }
        Files.write(targetFolder.resolve(COMBINED_FILE_NAME), combinedLines, StandardCharsets.UTF_8);

        // Construct and write the 'ignored' file.
        List<String> ignoredLines = new LinkedList<>();
        for(GeneratedAccessPoint sensor : metadata.ignoredAccessPoints){
            ignoredLines.add(sensor.name);
        }
        Files.write(targetFolder.resolve(IGNORED_FILE_NAME), ignoredLines, StandardCharsets.UTF_8);
    }

    /**
     * Writes the entry-files to disk. Data is generated and written to disk using the given
     * EntryGenerator one day at a time from the given start-date to the end-date (exclusive)
     */
    public void writeEntries(EntryGenerator entryGenerator, GeneratedMetadata metadata, LocalDate startDate, LocalDate endDate) throws IOException{ // Generated entries.
        List<EntryGenerator.AssignedDataSource> dataSources = entryGenerator.assignDataSources(metadata.accessPoints);

        // Construct and write the 'idmap' file.
        List<String> idmapLines = new LinkedList<>();
        for(Map.Entry<String, String> entry : entryGenerator.getIdMap().entrySet()){
            String id = entry.getKey();
            String sensorName = entry.getValue();
            String line = id + csvSeparator + sensorName;
            idmapLines.add(line);
        }
        Files.write(targetFolder.resolve(IDMAP_FILE_NAME), idmapLines, StandardCharsets.UTF_8);

        // Generate, construct, and write the files for the specified dates.
        LocalDate date = startDate;
        while(date.isBefore(endDate)){
            List<GeneratedEntry> entries = entryGenerator.generateEntries(date, dataSources);

            List<String> dateLines = new LinkedList<>();
            for(GeneratedEntry entry : entries){
                StringBuilder sb = new StringBuilder();
                sb.append("Time");
                sb.append(csvSeparator);
                sb.append(entry.time);
                sb.append(System.lineSeparator());
                sb.append("Total clients");
                sb.append(csvSeparator);
                sb.append(entry.totalClients);
                sb.append(System.lineSeparator());
                if(entry.data.isEmpty()){
                    sb.append("NO DATA");
                } else {
                    boolean first = true;
                    for(Map.Entry<String, Double> dataEntry : entry.data.entrySet()){
                        if(first){
                            first = false;
                        } else {
                            sb.append(System.lineSeparator());
                        }
                        String sensorName = dataEntry.getKey();
                        Double sensorProbability = dataEntry.getValue();
                        sb.append(sensorName);
                        sb.append(csvSeparator);
                        sb.append(sensorProbability);
                    }
                }

                dateLines.add(sb.toString());
            }

            Files.write(targetEntryFolder.resolve(date + ".csv"), dateLines, StandardCharsets.UTF_8);
            date = date.plusDays(1);
        }
    }
}
