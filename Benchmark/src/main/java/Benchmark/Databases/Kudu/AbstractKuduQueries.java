package Benchmark.Databases.Kudu;

import Benchmark.Config.Granularity;
import Benchmark.Queries.IQueries;
import Benchmark.Queries.Results.AvgOccupancy;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

public abstract class AbstractKuduQueries implements IQueries {
    protected KuduClient kuduClient;
    protected KuduTable kuduTable;
    protected Schema kuduSchema;

    protected long convertLocalDateTimeToMicrosecondLong(LocalDateTime dateTime){
        return Granularity.MICROSECOND.getTime(dateTime);
    }

    protected LocalDateTime convertMicrosecondsSinceEpochToLocalDateTime(long time){
        final int microsInOneSecond = 1_000_000;
        final int nanosInOneMicrosecond = 1_000;

        long secondsSinceEpoch = time / microsInOneSecond;
        assert (time - secondsSinceEpoch * microsInOneSecond) * nanosInOneMicrosecond
                == (int) (time - secondsSinceEpoch * microsInOneSecond) * nanosInOneMicrosecond
                : "Arithmetic overflow. Should never happen be able to happen.";
        int nanos = (int) (time - secondsSinceEpoch * microsInOneSecond) * nanosInOneMicrosecond;

        return LocalDateTime.ofEpochSecond(secondsSinceEpoch, nanos, ZoneOffset.ofHours(0));
    }

    protected KuduScanner.KuduScannerBuilder addTimeComparisonPredicates(KuduScanner.KuduScannerBuilder builder, LocalDateTime start, LocalDateTime end){
        return builder
                .addPredicate(KuduPredicate.newComparisonPredicate(
                        kuduSchema.getColumn("time"),
                        KuduPredicate.ComparisonOp.LESS,
                        convertLocalDateTimeToMicrosecondLong(end)))
                .addPredicate(KuduPredicate.newComparisonPredicate(
                        kuduSchema.getColumn("time"),
                        KuduPredicate.ComparisonOp.GREATER_EQUAL,
                        convertLocalDateTimeToMicrosecondLong(start)));
    }

    protected interface GroupByCompletion {
        void run(String timestamp, Integer queryResult);
    }
    protected interface GroupByComputation {
        int run(int previousValue, RowResult result);
    }

    protected void groupByDayAndCompute(LocalDateTime start, LocalDateTime end, KuduScanner scanner, GroupByComputation computation, GroupByCompletion completion) throws KuduException{
        //Kudu stores timestamps as microseconds. 1 second = 1_000_000 microseconds
        final int granularity = 1_000_000;

        assert (int) start.until(end, ChronoUnit.DAYS) == start.until(end, ChronoUnit.DAYS) : "Number of days between start and end overflows an int. WTF are you doing?";
        LocalDateTime startTrunc = start.truncatedTo(ChronoUnit.DAYS);
        int dayDiff = (int) startTrunc.until(end, ChronoUnit.DAYS);
        assert dayDiff >= 0 : "Did you mess up the order of start and end?";
        dayDiff++;

        long[] dayTimes = new long[dayDiff];
        LocalDateTime date = startTrunc;
        int index = 0;
        // The date.isEqual check is needed to be inclusive on the end-time, for the edge-case where "end == end.truncatedTo(ChronoUnit.DAYS)"
        while(date.isBefore(end) || date.isEqual(end)){
            dayTimes[index] = date.toEpochSecond(ZoneOffset.ofHours(0)) * granularity;
            date = date.plusDays(1);
            index++;
        }

        HashMap<Long, Integer> map = new HashMap<>();
        for (long dayTime : dayTimes) {
            assert dayTime != 0 : "0 is in the long-array. There is an off-by-one error somewhere";
            map.put(dayTime, 0);
        }

        while(scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while(results.hasNext()){
                RowResult result = results.next();
                assert kuduSchema.getColumnId("time") == 0 : "Has the Kudu schema been changed?";
                long time = result.getLong(0);
                // With 1 entry in the array, we only have 1 bucket which must be the correct bucket so this is a safe initial value.
                long bucket = dayTimes[0];
                for(int i = 0; i < dayTimes.length - 1; i++){
                    // We need to be inclusive on the start here because the end value belongs to a different day.
                    if (time >= dayTimes[i] && time < dayTimes[i+1]) {
                        bucket = dayTimes[i];
                        break;
                    }
                }
                // Special case for handling the final day. We could check "last day time <= time < end",
                // but the database-query already ensured that the last condition holds.
                //   (in reality the database query ensures that "... time <= end", however that distinction only matters
                //    in the case where end is exactly midnight, and that has been handled by previous logic since that
                //    is a new day and in that case "time = end" so we end up being inclusive when it matters)
                if(dayTimes.length > 1 && dayTimes[dayTimes.length - 1] <= time){
                    bucket = dayTimes[dayTimes.length-1];
                }

                assert map.containsKey(bucket) : "Invalid bucket " + bucket + " from time-long " + time +
                        ". Start " + start.toEpochSecond(ZoneOffset.ofHours(0)) * granularity +
                        ", end " + end.toEpochSecond(ZoneOffset.ofHours(0)) * granularity;
                int value = map.get(bucket);
                int newValue = computation.run(value, result);
                map.put(bucket, newValue);
            }
        }

        for(Map.Entry<Long, Integer> queryResult : map.entrySet()){
            LocalDateTime stamp = LocalDateTime.ofEpochSecond(queryResult.getKey() / granularity, 0, ZoneOffset.ofHours(0));
            completion.run(stamp.toString(), queryResult.getValue());
        }
    }

    @Override
    public LocalDateTime getNewestTimestamp(LocalDateTime previousNewestTime) throws KuduException {
        List<String> projectedColumns = new ArrayList<>();
        projectedColumns.add("time");

        long oldTime = convertLocalDateTimeToMicrosecondLong(previousNewestTime);

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner scanner = scannerBuilder
                .setProjectedColumnNames(projectedColumns)
                .addPredicate(KuduPredicate.newComparisonPredicate(
                        kuduSchema.getColumn("time"),
                        KuduPredicate.ComparisonOp.GREATER,
                        oldTime
                ))
                .build();

        long newestTime = oldTime;
        while(scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while(results.hasNext()){
                RowResult result = results.next();
                long time = result.getLong("time");
                if (time > newestTime){
                    newestTime = time;
                }
            }
        }

        LocalDateTime newTime = convertMicrosecondsSinceEpochToLocalDateTime(newestTime);
        if(newTime.isAfter(previousNewestTime)){
            // Add 1 second to match Timescale and Influx behavior.
            return newTime.plusSeconds(1);
        }
        // The previous time already has the second added, so dont add another second.
        return previousNewestTime;
    }

    protected List<AvgOccupancy> computeAvgOccupancyGivenProjectedColumns(LocalDateTime start, LocalDateTime end,
                                                                          int windowSizeInMin, int sampleRate,
                                                                          List<String> projectedColumns) throws KuduException{
        List<KuduScanner.KuduScannerBuilder> builders_q1 = new LinkedList<>();
        List<KuduScanner.KuduScannerBuilder> builders_q2 = new LinkedList<>();
        KuduScanner.KuduScannerBuilder builder_q3 = kuduClient.newScannerBuilder(kuduTable);

        // Step 1: Create the scanner-builders
        {
            LocalDateTime date = end;
            do {
                KuduScanner.KuduScannerBuilder builder = kuduClient.newScannerBuilder(kuduTable);
                builder = builder.setProjectedColumnNames(projectedColumns);
                builder = addTimeComparisonPredicates(builder, date.minusMinutes(windowSizeInMin), date);
                builders_q1.add(builder);
                date = date.minusDays(1);
            } while (!date.isBefore(start));
        }
        {
            LocalDateTime date = end;
            do {
                KuduScanner.KuduScannerBuilder builder = kuduClient.newScannerBuilder(kuduTable);
                builder = builder.setProjectedColumnNames(projectedColumns);
                builder = addTimeComparisonPredicates(builder, date, date.plusMinutes(windowSizeInMin));
                builders_q2.add(builder);
                date = date.minusDays(1);
            } while (!date.isBefore(start));
        }
        builder_q3 = addTimeComparisonPredicates(builder_q3, end.minusMinutes(sampleRate), end);

        // Step 2:  Scan and parse
        Map<String, List<Integer>> parsed_q1 = new HashMap<>();
        Map<String, List<Integer>> parsed_q2 = new HashMap<>();
        Map<String, Integer> parsed_q3 = new HashMap<>();

        for(KuduScanner.KuduScannerBuilder builder : builders_q1){
            KuduScanner scanner = builder.build();
            scanAndHandle(scanner, (value, AP) -> {
                List<Integer> prevValues = parsed_q1.computeIfAbsent(AP, k -> new ArrayList<>(130));
                prevValues.add(value);
            });
            scanner.close();
        }

        for(KuduScanner.KuduScannerBuilder builder : builders_q2){
            KuduScanner scanner = builder.build();
            scanAndHandle(scanner, (value, AP) -> {
                List<Integer> prevValues = parsed_q2.computeIfAbsent(AP, k -> new ArrayList<>(130));
                prevValues.add(value);
            });
            scanner.close();
        }

        KuduScanner scanner_q3 = builder_q3.build();
        scanAndHandle(scanner_q3, (value, AP) -> parsed_q3.put(AP,value));
        scanner_q3.close();

        List<AvgOccupancy> output = new ArrayList<>();

        // Step3: Do the averaging in-application.
        for(String name : parsed_q3.keySet()){
            // If we're handling an Avg Occupancy call from the row-schema, then APs mey be missing from our result due to 2 reasons:
            // #1: That particular AP has no entry for the queried time interval, such as if that particular AP crashed.
            // #2: There were no entries at all for the queried time interval, either because the seed-system was down or
            //    because the queried interval is too small.
            // For a column-based schema, #1 cannot happen because all APs are always present. However, the column-schema
            // is still susceptible to #2
            if(!(parsed_q1.containsKey(name) && parsed_q2.containsKey(name))){
                continue;
            }

            List<Integer> vals_q1 = parsed_q1.get(name);
            List<Integer> vals_q2 = parsed_q2.get(name);
            if(vals_q1.isEmpty() || vals_q2.isEmpty()) continue;
            int sum_q1 = 0;
            int sum_q2 = 0;
            for(int value : vals_q1){
                sum_q1 += value;
            }
            for(int value : vals_q2){
                sum_q2 += value;
            }
            double avg_q1 =  sum_q1 / (double) vals_q1.size();
            double avg_q2 =  sum_q2 / (double) vals_q2.size();

            output.add(new AvgOccupancy(name, parsed_q3.get(name), avg_q1, avg_q2));
        }

        return output;
    }

    protected interface AvgHandler{
        void run(int value, String AP);
    }

    protected abstract void scanAndHandle(KuduScanner scanner, AvgHandler handler) throws KuduException;
}
