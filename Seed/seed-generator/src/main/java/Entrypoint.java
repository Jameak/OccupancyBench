import Generator.EntryGenerator;
import Generator.Generated.GeneratedMetadata;
import Generator.MetadataGenerator;
import Generator.Serializer;
import picocli.CommandLine;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Random;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "Sample seed generator", mixinStandardHelpOptions = true,
        description = "Seed data generator to create sample data for the benchmark.")
public class Entrypoint implements Callable<Integer>
{
    @CommandLine.Option(names = {"-f", "--floors"}, required = true, paramLabel = "NUM",
            description = "number of floors to generate")
    private int floorsToGenerate;

    @CommandLine.Option(names = {"-s", "--sensors-per-floor"}, required = true, paramLabel = "NUM",
            description = "number of sensors to generate per floor")
    private int sensorsPerFloor;

    @CommandLine.Option(names = {"--gen-special"}, defaultValue = "true",
            description = "include a 'special' floor in the generated data (default: ${DEFAULT-VALUE})")
    private boolean generateSpecialFloorSample;

    @CommandLine.Option(names = {"--rng"}, defaultValue = "1234", paramLabel = "SEED",
            description = "the rng seed to use (default: ${DEFAULT-VALUE})")
    private int rngSeed;

    @CommandLine.Option(names = {"--sample-rate"}, defaultValue = "60", paramLabel = "NUM",
            description = "the sampling rate of the generated data in seconds (default: ${DEFAULT-VALUE})")
    private int sampleRate;

    @CommandLine.Option(names = {"--create-idmap"}, defaultValue = "true",
            description = "build an id-map during generation to reduce file-size of resulting output-files (default: ${DEFAULT-VALUE})")
    private boolean createIdMap;

    @CommandLine.Option(names = {"--peak-factor"}, defaultValue = "0.5", paramLabel = "SCALE",
            description = "a scaling factor to apply to the connected clients value (default: ${DEFAULT-VALUE})")
    private double peakScaleFactor;

    @CommandLine.Option(names = {"--out-folder"}, defaultValue = ".", paramLabel = "FOLDER",
            description = "target folder for output data (default: ${DEFAULT-VALUE})")
    private Path outputFolder;

    @CommandLine.Option(names = {"--time-start"}, defaultValue = "2020-06-15", paramLabel = "DATE",
            description = "start date to generate data for (inclusive) (default: ${DEFAULT-VALUE})")
    private LocalDate startDate;

    @CommandLine.Option(names = {"--time-end"}, defaultValue = "2020-06-22", paramLabel = "DATE",
            description = "end date to generate data for (exclusive) (default: ${DEFAULT-VALUE})")
    private LocalDate endDate;

    @CommandLine.Option(names = {"--separator"}, defaultValue = ";", paramLabel = "SEP",
            description = "separator to use in the output csv files (default: ${DEFAULT-VALUE})")
    private String csvSeparator;

    @CommandLine.Option(names = {"--delete-existing-entries"}, defaultValue = "false",
            description = "delete the target folder for date-entries if it already exists before writing data to it (default: ${DEFAULT-VALUE})")
    private boolean deleteExisting;

    public static void main(String... args)
    {
        int exitCode = new CommandLine(new Entrypoint()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        if(floorsToGenerate < 1){
            System.out.println("At least 1 floor must be generated");
            return 1;
        }
        if(sensorsPerFloor < 2){
            System.out.println("At least 2 sensors per floor must be generated");
            return 1;
        }
        if(sampleRate < 1){
            System.out.println("Sample-rate must be positive");
            return 1;
        }
        if(peakScaleFactor < 0.0){
            System.out.println("Scale-factor must be positive");
            return 1;
        }
        if(!outputFolder.toFile().exists()){
            System.out.println("Output folder " + outputFolder + " doesn't exist.");
            return 1;
        }
        if(!(startDate.isBefore(endDate))){
            System.out.println("Start date " + startDate + " must be before end date " + endDate);
            return 1;
        }

        Random rng = new Random(rngSeed);
        MetadataGenerator metaGen = new MetadataGenerator(floorsToGenerate,sensorsPerFloor,generateSpecialFloorSample, rng.nextInt());
        GeneratedMetadata metadata = metaGen.generate();
        EntryGenerator entryGen = new EntryGenerator(sampleRate, 1,true, true, createIdMap, peakScaleFactor, rng.nextInt());

        System.out.println("Generated seed data will be written to " + outputFolder);
        Serializer serializer = new Serializer(outputFolder, csvSeparator, deleteExisting);
        serializer.writeMetadata(metadata);
        serializer.writeEntries(entryGen, metadata, startDate, endDate);
        return 0;
    }
}
