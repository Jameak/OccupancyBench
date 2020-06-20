package Generator;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class EntryPatterns {
    public static final int PATTERN_LENGTH = 96;
    public static final int ROUNDS = 3;
    public static final int EXISTING_PATTERNS = 2;
    public static final int MAX_SHIFT_VALUE = 20; // Valid range is 2 <= MAX_SHIFT_VALUE < PATTERN_LENGTH - 1

    public static final int SHIFT_CHANCE = 25;  // Valid range is 0 <= SHIFT_CHANCE < 100
    public static final int COMBINE_CHANCE = 5; // Valid range is 0 <= COMBINE_CHANCE < 100
    public static final int SCALE_CHANCE = 25;  // Valid range is 0 <= SCALE_CHANCE < 100

    private static final int MINUTES_BETWEEN_ENTRIES = 15;
    private static final LocalTime LAST_PATTERN_TIME = LocalTime.of(23,45);
    private static final List<Integer> PATTERN_1 = Arrays.asList(
             0,   0,   0,  0, // 00:00-00:45
             0,   0,   0,  0, // 01:00-01:45
             0,   0,   0,  0, // 02:00-02:45
             0,   0,   0,  0, // 03:00-03:45
             0,   0,   1,  1, // 04:00-04:45
             1,   2,   1,  1, // 05:00-05:45
             2,   2,   2,  2, // 06:00-06:45
             5,   5,   6, 10, // 07:00-07:45
            10,  13,  13, 12, // 08:00-08:45
            12,  15,  20, 12, // 09:00-09:45
            13,  20,  13, 12, // 10:00-10:45
            30,  35,  34, 32, // 11:00-11:45
            25,  16,  13, 10, // 12:00-12:45
            10,  11,  12, 10, // 13:00-13:45
            12,   9,  10, 11, // 14:00-14:45
            15,  17,  16, 14, // 15:00-15:45
            13,   5,   4,  5, // 16:00-16:45
             7,   6,   6, 10, // 17:00-17:45
             4,   3,   5,  4, // 18:00-18:45
             2,   2,   1,  2, // 19:00-19:45
             1,   1,   2,  2, // 20:00-20:45
             2,   2,   2,  1, // 21:00-21:45
             1,   1,   0,  0, // 22:00-22:45
             0,   0,   0,  0  // 23:00-23:45
    );

    private static final List<Integer> PATTERN_2 = Arrays.asList(
             0,   0,   0,  0, // 00:00-00:45
             0,   0,   0,  0, // 01:00-01:45
             0,   0,   0,  0, // 02:00-02:45
             0,   0,   0,  0, // 03:00-03:45
             0,   0,   0,  0, // 04:00-04:45
             0,   0,   0,  0, // 05:00-05:45
             0,   1,   1,  5, // 06:00-06:45
             5,   8,   7,  9, // 07:00-07:45
            23,  24,  23, 23, // 08:00-08:45
            18,  24,  23, 25, // 09:00-09:45
            21,   7,   6,  4, // 10:00-10:45
             4,   3,   2,  4, // 11:00-11:45
             5,   5,   8, 10, // 12:00-12:45
            11,  12,  10, 10, // 13:00-13:45
            35,  40,  38, 37, // 14:00-14:45
            38,  36,  37, 41, // 15:00-15:45
            24,  12,  10,  8, // 16:00-16:45
             7,   4,   3,  2, // 17:00-17:45
             4,   3,   5,  4, // 18:00-18:45
             2,   2,   1,  2, // 19:00-19:45
             1,   1,   2,  2, // 20:00-20:45
             0,   0,   0,  0, // 21:00-21:45
             0,   0,   0,  0, // 22:00-22:45
             0,   0,   0,  0  // 23:00-23:45
    );

    private final TimeIndexPairing[] timeIndices;
    private final TimeIndexPairing firstTime;
    private int entriesBetweenPairingsAtSampleRate;

    public EntryPatterns(int sampleRate){
        timeIndices = new TimeIndexPairing[PATTERN_LENGTH];
        LocalTime time = LocalTime.of(0,0);
        for(int i = 0; i < PATTERN_LENGTH; i++){
            timeIndices[i] = new TimeIndexPairing(time, i);
            time = time.plusMinutes(MINUTES_BETWEEN_ENTRIES);
        }

        Arrays.sort(timeIndices, new TimeIndexComparator());

        firstTime = new TimeIndexPairing(LocalTime.of(0,0), 0);
        LocalTime secondTime = firstTime.time.plusMinutes(MINUTES_BETWEEN_ENTRIES);
        LocalTime timeVal = firstTime.time;
        while(timeVal.isBefore(secondTime)){
            // This doesn't properly handle the case where the given sample-rate is not evenly divisible by
            //   MINUTES_BETWEEN_ENTRIES, but this is just a simple generator for sample data so the impact that
            //   this will have on the already-bogus values that we generate doesn't matter.
            entriesBetweenPairingsAtSampleRate++;
            timeVal = timeVal.plusSeconds(sampleRate);
        }
    }

    /**
     * Retrieve a random pattern. This pattern is will be one of the hardcoded patterns
     * but modified by shifting, scaling and combining values.
     */
    public static int[] getRandomPattern(Random rng){
        assert PATTERN_LENGTH == PATTERN_1.size() && PATTERN_LENGTH == PATTERN_2.size() : "You fucked up the patterns";

        int[] values = getPattern(rng.nextInt(EXISTING_PATTERNS)+1);

        for(int i = 0; i < ROUNDS; i++){
            if(rng.nextInt(100) < SHIFT_CHANCE){
                shiftPattern(values, rng.nextInt(MAX_SHIFT_VALUE)-(MAX_SHIFT_VALUE/2));
            } else if(rng.nextInt(100) < COMBINE_CHANCE){
                combinePattern(values, getPattern(rng.nextInt(EXISTING_PATTERNS)+1));
            } else if(rng.nextInt(100) < SCALE_CHANCE){
                scalePattern(values, rng.nextDouble() + 0.5);
            }
        }

        return values;
    }

    private static void combinePattern(int[] patternToCombineInto, int[] secondaryPattern){
        for(int i = 0; i < patternToCombineInto.length; i++){
            patternToCombineInto[i] = patternToCombineInto[i] + secondaryPattern[i];
        }
    }

    private static void shiftPattern(int[] patternToShift, int shiftValue){
        assert shiftValue < PATTERN_LENGTH;
        int[] copy = new int[PATTERN_LENGTH];
        System.arraycopy(patternToShift, 0, copy, 0, patternToShift.length);

        int copyIndex = 0;
        int targetIndex = shiftValue < 0 ? patternToShift.length - 1 + shiftValue : shiftValue;
        while(copyIndex < copy.length){
            patternToShift[targetIndex] = copy[copyIndex];
            copyIndex++;
            targetIndex = (targetIndex + 1) % PATTERN_LENGTH;
        }
    }

    private static void scalePattern(int[] patternToScale, double scaleFactor){
        for(int i = 0; i < patternToScale.length; i++){
            patternToScale[i] = (int) Math.floor(patternToScale[i] * scaleFactor);
        }
    }

    private static int[] getPattern(int patternToSelect){
        switch (patternToSelect){
            case 1:
                return toArray(PATTERN_1);
            case 2:
                return toArray(PATTERN_2);
            default:
                throw new IllegalStateException("Unexpected value: " + patternToSelect);
        }
    }

    private static int[] toArray(List<Integer> pattern){
        int[] out = new int[PATTERN_LENGTH];
        for(int i = 0; i < pattern.size(); i++){
            out[i] = pattern.get(i);
        }
        return out;
    }

    /**
     * Given a way to convert from a timestamp to a pattern-index (the pairing-argument),
     * the pattern to source our data from (pattern) and the number of entries that
     * we've already generated at our chosen sample-rate since the first timestamp in
     * our pairing, interpolate a value that corresponds to the next entry within this pairing.
     */
    public int getValueForTime(TimeIndexPairing[] pairing, int[] pattern, int entriesSinceFirstPairing){
        assert pairing.length == 2;

        if(pairing[0].equals(pairing[1])) {
            return pattern[pairing[0].index];
        } else {
            double blendValPerEntry = 1.0 / (entriesBetweenPairingsAtSampleRate + 1);
            return (int) Math.ceil(linearInterpolate(pattern[pairing[0].index], pattern[pairing[1].index], blendValPerEntry * (entriesSinceFirstPairing + 1)));
        }
    }


    /**
     * Get the time-index pair that most closely fits the provided timestamp.
     * This can either be two time-index objects where:
     *     out[0] <= time < out[1]
     * or we can find an exact match which then gives us
     *     out[0] == time == out[1] (where out[0] == out[1])
     */
    public TimeIndexPairing[] getPairing(LocalTime time){
        // TODO: This could be done more efficiently with a binary search but that
        //       is unneeded for the number of values we have in our array.
        TimeIndexPairing[] out = new TimeIndexPairing[2];
        for(int i = 0; i < PATTERN_LENGTH; i++){
            if(timeIndices[i].time.compareTo(time) == 0){
                out[0] = timeIndices[i];
                out[1] = timeIndices[i];
                return out;
            } else if(timeIndices[i].time.compareTo(time) > 0){
                out[1] = timeIndices[i];
                return out;
            } else {
                out[0] = timeIndices[i];
            }
        }

        assert out[0].time.equals(LAST_PATTERN_TIME) : "No match found earlier, and final value isn't the final entry in the time-index thing... what?";
        out[1] = firstTime;
        return out;
    }

    private static double linearInterpolate(double v1, double v2, double blend){
        // For this generator we dont want to interpolate down to 0. Just do a hard cut instead. Doesn't really matter
        // since this is clearly fake data anyway, but does improve the real-life resemblance of the data slightly.
        if(v1 == 0.0 || v2 == 0.0) return 0.0;

        return ((1 - blend) * v1 + blend * v2);
    }

    // @NOTE: This is probably unnecessary since we could just do a LocalTime[] where the index of that array is the
    //        index that we use in the pattern-array. However, this is slightly clearer and doesn't rely on ordering-
    //        assumptions of LocalTime... though those ordering assumptions wouldn't be an issue anyway. Meh.
    public static class TimeIndexPairing{
        public final LocalTime time;
        public final int index;

        public TimeIndexPairing(LocalTime time, int index){
            this.time = time;
            this.index = index;
        }
    }

    private static class TimeIndexComparator implements Comparator<TimeIndexPairing>{
        @Override
        public int compare(TimeIndexPairing o1, TimeIndexPairing o2) {
            return o1.time.compareTo(o2.time);
        }
    }
}
