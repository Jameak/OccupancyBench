package Benchmark.Generator;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class DataGenerator {
    public static List<GeneratedEntry> Generate(int interval, int loadedInterval, Floor[] generatedFloors, MapData data, LocalDate startDate, LocalDate endDate, double scale, Random rng){
        boolean timeMatch = interval == loadedInterval;
        // These things assume timeMatch is true for now.
        assert timeMatch; //TODO: Handle the case where timeMatch isn't true.

        List<GeneratedEntry> generatedEntries = new LinkedList<>();

        LocalDate[] sortedEntryKeys = data.getDateEntries().keySet().toArray(new LocalDate[0]);
        Arrays.sort(sortedEntryKeys);


        // 1. Do initial distribution of clients to the APs.
        // 2. Every timestep, we have a new probability-distribution, so:
        //    2.1 Switch the probability
        //    2.2 Then move clients in overcrowded APs to adjacent APs (preferring partner-APs. Clients have chance of disappearing, jumping APs entirely or new clients appearing out of nowhere)

        int startSecond = rng.nextInt(10) + 1;
        // Generate initial distribution
        Entry firstEntry = data.getDateEntries().get(sortedEntryKeys[0])[0];

        LocalTime startReadingTime = LocalTime.of(0,0,startSecond,0);
        LocalTime previousReadingTime = startReadingTime;
        for(Floor floor : generatedFloors){
            AccessPoint[] APs = floor.getAPs();
            for(AccessPoint AP : APs){
                int APid = AP.getMapID();
                double probability = firstEntry.hasData() ? firstEntry.getProbabilities().get(APid) : 0.0;

                int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                LocalTime readingTime = previousReadingTime.plusNanos(nanoSecondsBetweenReadings);

                generatedEntries.add(new GeneratedEntry(startDate.toString() + "T" + readingTime.toString() + "Z", AP.getAPname(), (int)Math.ceil(firstEntry.getTotal() * scale * probability)));
                previousReadingTime = readingTime;
            }
        }

        // Every timestep, do...
        startReadingTime = startReadingTime.plusSeconds(interval);
        LocalDate nextDate = startDate;
        for(int i = 1; i < sortedEntryKeys.length; i++){
            previousReadingTime = startReadingTime;
            Entry[] entriesOnDate = data.getDateEntries().get(sortedEntryKeys[i]);
            for(Entry entryOnDay : entriesOnDate){
                for(Floor floor : generatedFloors){
                    AccessPoint[] APs = floor.getAPs();
                    for(AccessPoint AP : APs){
                        //TODO: For now, this just matches the actual data 1-to-1, but with random AP assignments and scaled as desired.
                        //  So it completely ignores AP-adjacency, partners, etc. Add the random walk here to introduce more randomness.

                        int APid = AP.getMapID();
                        double probability = entryOnDay.hasData() ? entryOnDay.getProbabilities().get(APid) : 0.0;

                        int nanoSecondsBetweenReadings = 15_000_000 + rng.nextInt(10_000_000);
                        LocalTime readingTime = previousReadingTime.plusNanos(nanoSecondsBetweenReadings);

                        generatedEntries.add(new GeneratedEntry(nextDate.toString() + "T" + readingTime.toString() + "Z", AP.getAPname(), (int)Math.ceil(entryOnDay.getTotal() * scale * probability)));
                        previousReadingTime = readingTime;
                    }
                }
            }

            nextDate = nextDate.plusDays(1);
            if(nextDate == endDate) break;
        }

        if(nextDate.isBefore(endDate)){
            //TODO: Continue generating.
        }


        return generatedEntries;
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
