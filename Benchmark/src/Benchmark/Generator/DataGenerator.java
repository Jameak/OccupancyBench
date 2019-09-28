package Benchmark.Generator;

import Benchmark.Generator.Targets.ITarget;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Random;

public class DataGenerator {
    public static void Generate(int interval, int loadedInterval, Floor[] generatedFloors, MapData data, LocalDate startDate, LocalDate endDate, double scale, Random rng, ITarget outputTarget) throws IOException {
        boolean timeMatch = interval == loadedInterval;
        // These things assume timeMatch is true for now.
        assert timeMatch; //TODO: Handle the case where timeMatch isn't true.
        assert startDate.isBefore(endDate);

        LocalDate[] sortedEntryKeys = data.getDateEntries().keySet().toArray(new LocalDate[0]);
        Arrays.sort(sortedEntryKeys);

        int startSecond = rng.nextInt(10) + 1;

        LocalDate nextDate = startDate;
        while(!nextDate.isAfter(endDate)){ // If we run out of data to generate from, then go back to the beginning.
            nextDate = GenerateEntries(nextDate, endDate, startSecond, interval, loadedInterval, generatedFloors, sortedEntryKeys, data, rng, scale, outputTarget);
        }
    }

    private static LocalDate GenerateEntries(LocalDate startDate, LocalDate endDate, int startSecond, int interval, int loadedInterval, Floor[] generatedFloors, LocalDate[] sortedEntryKeys, MapData data, Random rng, double scale, ITarget outputTarget) throws IOException {
        LocalTime previousReadingTime;
        LocalDate nextDate = startDate;

        for(int i = 0; i < sortedEntryKeys.length; i++){
            LocalTime startTime = LocalTime.of(0,0,startSecond,0);

            Entry[] entriesOnDate = data.getDateEntries().get(sortedEntryKeys[i]);
            for(Entry entryOnDay : entriesOnDate){
                previousReadingTime = startTime;
                for(Floor floor : generatedFloors){
                    AccessPoint[] APs = floor.getAPs();
                    for(AccessPoint AP : APs){
                        //TODO: For now, this just matches the actual data 1-to-1, but with random AP assignments and scaled as desired.
                        //  So it completely ignores AP-adjacency, partners, etc. Add the random walk here to introduce more randomness.

                        int APid = AP.getMapID();
                        if(!entryOnDay.hasData()) continue; // Entry has no data, so generate nothing rather than zeros.
                        if(entryOnDay.getProbabilities().get(APid) == null) continue; // Entry has data, but no data for this specific AP. So rather than generating a 0, we create a hole in the data, just like in the source-data.
                        double probability = entryOnDay.getProbabilities().get(APid);

                        int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                        LocalTime readingTime = previousReadingTime.plusNanos(nanoSecondsBetweenReadings);

                        GeneratedEntry genEntry = new GeneratedEntry(nextDate.toString() + "T" + readingTime.toString() + "Z", AP.getAPname(), (int)Math.ceil(entryOnDay.getTotal() * scale * probability));
                        outputTarget.add(genEntry);
                        previousReadingTime = readingTime;
                    }
                }
                startTime = startTime.plusSeconds(interval);
            }

            nextDate = nextDate.plusDays(1);
            if(nextDate.isEqual(endDate)) break;
        }

        return nextDate;
    }

    public static class GeneratedEntry{
        private final String timestamp;
        private final String ap;
        private final int numClients;

        public GeneratedEntry(String timestamp, String AP, int numClients){
            this.timestamp = timestamp;
            ap = AP;
            this.numClients = numClients;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getAP() {
            return ap;
        }

        public int getNumClients() {
            return numClients;
        }

        @Override
        public String toString() {
            return timestamp + ";" + ap + ";" + numClients;
        }
    }
}
