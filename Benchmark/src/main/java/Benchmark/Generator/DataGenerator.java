package Benchmark.Generator;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.SchemaFormats;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedColumnEntry;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.Loader.Entry;
import Benchmark.Loader.MapData;
import Benchmark.Generator.Targets.ITarget;

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
     * The given config file controls the scale and interval of the generated data.
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
    public static void Generate(AccessPoint[] APs, MapData data, LocalDate startDate, LocalDate endDate, Random rng,
                                ITarget outputTarget, ConfigFile config) throws IOException, SQLException {
        int interval = config.getGeneratorGenerationInterval();
        int loadedInterval = config.getGeneratorSourceInterval();
        double scale = config.getGeneratorScale();
        int jitterMax = config.getGeneratorJitter();

        assert interval == loadedInterval || // Intervals match
                (interval < loadedInterval && loadedInterval % interval == 0) || // Interval to generate is quicker than data-interval. Then the generate-interval must be evenly divisible by the data-interval
                (interval > loadedInterval && interval % loadedInterval == 0) :  // Interval to generate is slower than data-interval.  Then the data-interval must be evenly divisible by the generate-interval
                "Mismatching intervals. Intervals must match, or one interval must be evenly divisible by the other.\n Interval: " + interval + ". LoadedInterval: " + loadedInterval;

        assert startDate.isBefore(endDate);

        LocalDate[] sortedEntryKeys = data.getDateEntries().keySet().toArray(new LocalDate[0]);
        Arrays.sort(sortedEntryKeys);

        int startSecond = rng.nextInt(10) + 1;

        LocalDate nextDate = startDate;
        while(!nextDate.isAfter(endDate)){ // If we run out of data to generate from, then go back to the beginning.
            nextDate = GenerateEntries(nextDate, endDate, startSecond, interval, loadedInterval, APs, sortedEntryKeys, data, rng, scale, outputTarget, jitterMax, config.getSchema(), config.DEBUG_synchronizeRngState());
            if(nextDate.isEqual(endDate) || outputTarget.shouldStopEarly()) break;
        }
    }

    private static LocalDate GenerateEntries(LocalDate startDate, LocalDate endDate, int startSecond, int interval,
                                             int loadedInterval, AccessPoint[] APs, LocalDate[] sortedEntryKeys,
                                             MapData data, Random rng, double scale, ITarget outputTarget,
                                             int jitterMax, SchemaFormats schema, boolean DEBUG_sync_rng_state)
                                             throws IOException, SQLException {
        boolean generateFasterThanLoadedData = interval < loadedInterval;
        boolean generateSlowerThanLoadedData = interval > loadedInterval;

        LocalDate nextDate = startDate;
        int numEntriesToSkip = (interval / loadedInterval) - 1; // Example: (120 / 60) - 1 = 1. Skip 1 entry every loop
        int numAdditionalEntriesToInclude = (loadedInterval / interval) - 1; // Example: (60 / 20) - 1 = 2. Add 2 extra entries every loop

        for (int k = 0; k < sortedEntryKeys.length; k++) {
            if(outputTarget.shouldStopEarly()) break;

            LocalDate sortedEntryKey = sortedEntryKeys[k];
            LocalTime startTime = LocalTime.of(0, 0, startSecond, 0);

            Entry[] entriesOnDate = data.getDateEntries().get(sortedEntryKey);
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

                GenerateBasedOnEntry(startTime, APs, entryOnDay, rng, nextDate, scale, outputTarget, jitterMax, schema, DEBUG_sync_rng_state);

                if (generateFasterThanLoadedData) {
                    Entry nextEntry = null;
                    boolean noData = false;
                    if (i + 1 < entriesOnDate.length) { // Next entry is just the next one on this day
                        nextEntry = entriesOnDate[i + 1];
                    } else if(k+1 < sortedEntryKeys.length) { // No more entries to interpolate off of today. Grab the first entry from the next day
                        assert data.getDateEntries().get(sortedEntryKeys[k + 1]) != null && data.getDateEntries().get(sortedEntryKeys[k + 1]).length > 0;
                        nextEntry = data.getDateEntries().get(sortedEntryKeys[k + 1])[0];
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
                            startTime = startTime.plusSeconds(interval);
                            Entry fakeEntry = CreateFakeEntry(entryOnDay, nextEntry, j, numAdditionalEntriesToInclude);
                            GenerateBasedOnEntry(startTime, APs, fakeEntry, rng, nextDate, scale, outputTarget, jitterMax, schema, DEBUG_sync_rng_state);
                        }
                    } else {
                        startTime = startTime.plusSeconds(interval * numAdditionalEntriesToInclude);
                    }
                }
                startTime = startTime.plusSeconds(interval);
            }

            nextDate = nextDate.plusDays(1);
            if (nextDate.isEqual(endDate) || nextDate.isAfter(endDate)) break;
        }

        return nextDate;
    }

    private static void GenerateBasedOnEntry(LocalTime startTime, AccessPoint[] APs, Entry entryOnDay, Random rng,
                                             LocalDate nextDate, double scale, ITarget outputTarget, int jitterMax,
                                             SchemaFormats schema, boolean DEBUG_sync_rng_state) throws IOException, SQLException {
        LocalTime readingTime = startTime;
        switch (schema){
            case ROW:
                for (AccessPoint AP : APs) {
                    int APid = AP.getMapID();
                    if (!entryOnDay.hasData())
                        continue; // Entry has no data, so generate nothing rather than zeros.
                    if (entryOnDay.getProbabilities().get(APid) == null)
                        continue; // Entry has data, but no data for this specific AP. So rather than generating a 0, we create a hole in the data, just like in the source-data.
                    double probability = entryOnDay.getProbabilities().get(APid);

                    int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                    readingTime = readingTime.plusNanos(nanoSecondsBetweenReadings);

                    int numClients;
                    if(jitterMax == 0){
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * scale * probability));
                    } else {
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * scale * probability) + Math.ceil(rng.nextInt(jitterMax) * probability));
                    }
                    GeneratedRowEntry genEntry = new GeneratedRowEntry(nextDate, readingTime, AP.getAPname(), numClients);
                    outputTarget.add(genEntry);
                }
                break;
            case COLUMN:
                HashMap<String, Integer> entries = new HashMap<>();

                if (!DEBUG_sync_rng_state){
                    int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                    readingTime = readingTime.plusNanos(nanoSecondsBetweenReadings);
                }

                // NOTE: Not a requirement that all APs are present in the "APs" variable.
                for (AccessPoint AP : APs) {
                    int APid = AP.getMapID();
                    if (!entryOnDay.hasData())
                        continue; // Entry has no data, so generate nothing (rather than a zero). The code that writes APs to the database needs to handle holes anyway.
                    if (entryOnDay.getProbabilities().get(APid) == null)
                        continue; // Entry has data, but no data for this specific AP. So rather than generating a 0, we create a hole in the data, just like in the source-data.
                    double probability = entryOnDay.getProbabilities().get(APid);

                    // To be able to directly compare generated data between schema-options, we need to keep the rng-state in sync
                    // and generate our values in the exact same way. This makes no sense from a performance-perspective, so
                    // only do this if the user explicitly requests it.
                    if(DEBUG_sync_rng_state){
                        int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                        readingTime = readingTime.plusNanos(nanoSecondsBetweenReadings);
                    }

                    int numClients;
                    if(jitterMax == 0){
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * scale * probability));
                    } else {
                        numClients = (int) (Math.ceil(entryOnDay.getTotal() * scale * probability) + Math.ceil(rng.nextInt(jitterMax) * probability));
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
        LocalDateTime time = null; // Time-field isn't used during generation. If that changes, this needs to be changed.

        double blendPerNum = 1.0 / (additionalEntries+1);
        int interpolatedTotal = (int)Math.ceil(LinearInterpolate(first.getTotal(), last.getTotal(), blendPerNum * (index+1)));

        Set<Integer> idsToInterpProbsFor = first.getProbabilities().keySet();
        Map<Integer, Double> interpProps = new HashMap<>();
        for(Integer id : idsToInterpProbsFor){
            double firstProp = first.getProbabilities().get(id);
            // Hole in AP-data for 'last'. Extend hole in fake entries.
            if(!last.getProbabilities().containsKey(id)) continue;

            double lastProp = last.getProbabilities().get(id);
            double interpProp = LinearInterpolate(firstProp, lastProp, blendPerNum * (index+1));
            interpProps.put(id, interpProp);
        }

        return new Entry(time, interpolatedTotal, interpProps);
    }

    private static double LinearInterpolate(double v1, double v2, double blend){
        return ((1 - blend) * v1 + blend * v2);
    }


}
