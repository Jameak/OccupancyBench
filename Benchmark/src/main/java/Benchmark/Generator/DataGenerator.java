package Benchmark.Generator;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.SchemaFormats;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedColumnEntry;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.SeedLoader.Seeddata.Entry;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.SeedLoader.Seeddata.SeedEntries;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * Generates data that is directly ready for insertion into a database, a file, etc.
 */
public class DataGenerator {
    /**
     * Generates data for the given APs, based on the given data, between the specified start-date (inclusive)
     * and end-date (exclusive). Generated data is added to the specified target.
     *
     * The given config file controls the scale and sample rate of the generated data.
     *
     * @param APs The APs for which to generate data.
     * @param data The data to use as the basis for generation.
     * @param startDate The date to start generation from. Inclusive
     * @param endDate The date to end generation on. Exclusive
     * @param rng The Random-instance to use during generation
     * @param outputTarget The target to add generated data to.
     * @param config The config file to use.
     * @throws IOException Thrown if the outputTarget throws when data is added.
     */
    public static void Generate(GeneratedAccessPoint[] APs, SeedEntries data, LocalDate startDate, LocalDate endDate, Random rng,
                                ITarget outputTarget, ConfigFile config) throws IOException, SQLException {
        int generatorSampleRate = config.getGeneratorGenerationSamplerate();
        int seedSampleRate = config.getGeneratorSeedSamplerate();
        double clientsScaleFactor = config.getGeneratorScaleFactorConnectedClients();
        int jitterMax = config.getGeneratorJitter();

        assert generatorSampleRate == seedSampleRate || // Intervals match
                (generatorSampleRate < seedSampleRate && seedSampleRate % generatorSampleRate == 0) || // Interval to generate is quicker than data-interval. Then the generate-interval must be evenly divisible by the data-interval
                (generatorSampleRate > seedSampleRate && generatorSampleRate % seedSampleRate == 0) :  // Interval to generate is slower than data-interval.  Then the data-interval must be evenly divisible by the generate-interval
                "Mismatching sample rates. Sample rates must match, or one sample rate must be evenly divisible by the other.\n Generator sample rate: " + generatorSampleRate + ". Seed sample rate: " + seedSampleRate;

        assert startDate.isBefore(endDate);

        LocalDate[] sortedEntryKeys = data.loadedEntries.keySet().toArray(new LocalDate[0]);
        Arrays.sort(sortedEntryKeys);

        int startSecond = rng.nextInt(10) + 1;

        LocalDate nextDate = startDate;
        while(!nextDate.isAfter(endDate)){ // If we run out of data to generate from, then go back to the beginning.
            nextDate = GenerateEntries(nextDate, endDate, startSecond, generatorSampleRate, seedSampleRate, APs, sortedEntryKeys, data, rng, clientsScaleFactor, outputTarget, jitterMax, config.getSchema(), config.DEBUG_synchronizeRngState());
            if(nextDate.isEqual(endDate) || outputTarget.shouldStopEarly()) break;
        }
    }

    private static LocalDate GenerateEntries(LocalDate startDate, LocalDate endDate, int startSecond, int generatorSampleRate,
                                             int seedSampleRate, GeneratedAccessPoint[] APs, LocalDate[] sortedEntryKeys,
                                             SeedEntries data, Random rng, double clientsScaleFactor, ITarget outputTarget,
                                             int jitterMax, SchemaFormats schema, boolean DEBUG_sync_rng_state)
                                             throws IOException, SQLException {
        boolean generateFasterThanLoadedData = generatorSampleRate < seedSampleRate;
        boolean generateSlowerThanLoadedData = generatorSampleRate > seedSampleRate;

        LocalDate nextDate = startDate;
        int numEntriesToSkip = (generatorSampleRate / seedSampleRate) - 1; // Example: (120 / 60) - 1 = 1. Skip 1 entry every loop
        int numAdditionalEntriesToInclude = (seedSampleRate / generatorSampleRate) - 1; // Example: (60 / 20) - 1 = 2. Add 2 extra entries every loop

        for (int k = 0; k < sortedEntryKeys.length; k++) {
            if(outputTarget.shouldStopEarly()) break;

            LocalDate sortedEntryKey = sortedEntryKeys[k];
            LocalTime startTime = LocalTime.of(0, 0, startSecond, 0);

            Entry[] entriesOnDate = data.loadedEntries.get(sortedEntryKey);
            int skippedEntries = numEntriesToSkip; // Set to numEntriesToSkip initially so that the first loop-iteration isn't skipped.
            for (int i = 0; i < entriesOnDate.length; i++) {
                Entry entryOnDay = entriesOnDate[i];

                // Skip entries to generate data slower than the source-data from the loaded entries
                if (generateSlowerThanLoadedData && skippedEntries != numEntriesToSkip) {
                    skippedEntries++;
                    continue;
                } else {
                    skippedEntries = 0;
                }

                GenerateBasedOnEntry(startTime, APs, entryOnDay, rng, nextDate, clientsScaleFactor, outputTarget, jitterMax, schema, DEBUG_sync_rng_state);

                if (generateFasterThanLoadedData) {
                    Entry nextEntry = null;
                    boolean noData = false;
                    if (i + 1 < entriesOnDate.length) { // Next entry is just the next one on this day
                        nextEntry = entriesOnDate[i + 1];
                    } else if(k+1 < sortedEntryKeys.length) { // No more entries to interpolate off of today. Grab the first entry from the next day
                        assert data.loadedEntries.get(sortedEntryKeys[k + 1]) != null && data.loadedEntries.get(sortedEntryKeys[k + 1]).length > 0;
                        nextEntry = data.loadedEntries.get(sortedEntryKeys[k + 1])[0];
                    } else {
                        // No more data left to base interpolation off of.
                        // In this case, just create a small hole instead.
                        noData = true;
                    }

                    // One entry has a hole so dont interpolate, instead just widen the hole.
                    if(!entryOnDay.hasData() || (nextEntry != null && !nextEntry.hasData())) noData = true;

                    if(!noData){
                        assert nextEntry != null;
                        for (int j = 0; j < numAdditionalEntriesToInclude; j++) {
                            startTime = startTime.plusSeconds(generatorSampleRate);
                            Entry fakeEntry = CreateFakeEntry(entryOnDay, nextEntry, j, numAdditionalEntriesToInclude);
                            GenerateBasedOnEntry(startTime, APs, fakeEntry, rng, nextDate, clientsScaleFactor, outputTarget, jitterMax, schema, DEBUG_sync_rng_state);
                        }
                    } else {
                        startTime = startTime.plusSeconds(generatorSampleRate * numAdditionalEntriesToInclude);
                    }
                }
                startTime = startTime.plusSeconds(generatorSampleRate);
            }

            nextDate = nextDate.plusDays(1);
            if (nextDate.isEqual(endDate) || nextDate.isAfter(endDate)) break;
        }

        return nextDate;
    }

    private static void GenerateBasedOnEntry(LocalTime startTime, GeneratedAccessPoint[] APs, Entry entryOnDay, Random rng,
                                             LocalDate nextDate, double clientsScaleFactor, ITarget outputTarget, int jitterMax,
                                             SchemaFormats schema, boolean DEBUG_sync_rng_state) throws IOException, SQLException {
        LocalTime readingTime = startTime;
        switch (schema){
            case NARROW:
                for (GeneratedAccessPoint AP : APs) {
                    String apSeedName = AP.getOriginalName();
                    if (!entryOnDay.hasData())
                        continue; // Entry has no data, so generate nothing rather than zeros.
                    if (entryOnDay.getProbabilities().get(apSeedName) == null)
                        continue; // Entry has data, but no data for this specific AP. So rather than generating a 0, we create a hole in the data.
                    double probability = entryOnDay.getProbabilities().get(apSeedName);

                    int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                    readingTime = readingTime.plusNanos(nanoSecondsBetweenReadings);

                    int numClients;
                    if(jitterMax == 0){
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * clientsScaleFactor * probability));
                    } else {
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * clientsScaleFactor * probability) + Math.ceil(rng.nextInt(jitterMax) * probability));
                    }
                    GeneratedRowEntry genEntry = new GeneratedRowEntry(nextDate, readingTime, AP.getAPname(), numClients);
                    outputTarget.add(genEntry);
                }
                break;
            case WIDE:
                HashMap<String, Integer> entries = new HashMap<>();

                if (!DEBUG_sync_rng_state){
                    int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                    readingTime = readingTime.plusNanos(nanoSecondsBetweenReadings);
                }

                // @NOTE: Not a requirement that all APs are present in the "APs" variable.
                for (GeneratedAccessPoint AP : APs) {
                    String apSeedName = AP.getOriginalName();
                    if (!entryOnDay.hasData())
                        continue; // Entry has no data, so generate nothing (rather than a zero). The code that writes APs to the database needs to handle holes anyway.
                    if (entryOnDay.getProbabilities().get(apSeedName) == null)
                        continue; // Entry has data, but no data for this specific AP. So rather than generating a 0, we create a hole in the data, just like in the source-data.
                    double probability = entryOnDay.getProbabilities().get(apSeedName);

                    // To be able to directly compare generated data between schema-options, we need to keep the rng-state in sync
                    // and generate our values in the exact same way. This makes no sense from a performance-perspective, so
                    // only do this if the user explicitly requests it.
                    if(DEBUG_sync_rng_state){
                        int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                        readingTime = readingTime.plusNanos(nanoSecondsBetweenReadings);
                    }

                    int numClients;
                    if(jitterMax == 0){
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * clientsScaleFactor * probability));
                    } else {
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * clientsScaleFactor * probability) + Math.ceil(rng.nextInt(jitterMax) * probability));
                    }
                    entries.put(AP.getAPname(), numClients);
                }

                outputTarget.add(new GeneratedColumnEntry(nextDate, readingTime, entries));
                break;
            default:
                assert false : "A schema was specified that isn't supported by data generator: " + schema;
                throw new IllegalStateException("Schema specified that isn't supported by data generator: " + schema);
        }
    }

    private static Entry CreateFakeEntry(Entry first, Entry last, int index, int additionalEntries){
        assert last != null;
        LocalDateTime time = null; // @NOTE: Time-field isn't used during generation. If that changes, this needs to be changed to give fake entries timestamps.

        double blendPerNum = 1.0 / (additionalEntries+1);
        int interpolatedTotal = (int)Math.ceil(LinearInterpolate(first.getTotal(), last.getTotal(), blendPerNum * (index+1)));

        Set<String> apsToInterpProbsFor = first.getProbabilities().keySet();
        Map<String, Double> interpProps = new HashMap<>();
        for(String name : apsToInterpProbsFor){
            double firstProp = first.getProbabilities().get(name);
            // Hole in AP-data for 'last'. In this case we extend the hole by not generating anything.
            if(!last.getProbabilities().containsKey(name)) continue;

            double lastProp = last.getProbabilities().get(name);
            double interpProp = LinearInterpolate(firstProp, lastProp, blendPerNum * (index+1));
            interpProps.put(name, interpProp);
        }

        return new Entry(time, interpolatedTotal, interpProps);
    }

    private static double LinearInterpolate(double v1, double v2, double blend){
        return ((1 - blend) * v1 + blend * v2);
    }


}
