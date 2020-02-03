package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.Queries.IQueries;
import Benchmark.Queries.Results.AvgOccupancy;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKuduQueries implements IQueries {
    protected KuduClient kuduClient;
    protected KuduTable kuduTable;
    protected Schema kuduSchema;

    protected Timestamp convertLocalDateTimeToTimestamp(LocalDateTime dateTime){
        //TODO: Refactor something so we can easily access the 'padTime' method in KuduRowTarget and
        //      'getTime' in AbstractGeneratedEntry to convert from the LocalDateTime to the Timestamp-value.
        long granularTime = new GeneratedRowEntry(dateTime.toLocalDate(), dateTime.toLocalTime(), "N/A", 0).getTime(ConfigFile.Granularity.MILLISECOND);
        return new Timestamp(ConfigFile.Granularity.MILLISECOND.toTimeUnit().convert(granularTime, TimeUnit.MILLISECONDS));
    }

    protected KuduScanner.KuduScannerBuilder addTimeComparisonPredicates(KuduScanner.KuduScannerBuilder builder, LocalDateTime start, LocalDateTime end){
        return builder
                .addPredicate(KuduPredicate.newComparisonPredicate(
                        kuduSchema.getColumn("time"),
                        KuduPredicate.ComparisonOp.LESS,
                        convertLocalDateTimeToTimestamp(end)))
                .addPredicate(KuduPredicate.newComparisonPredicate(
                        kuduSchema.getColumn("time"),
                        KuduPredicate.ComparisonOp.GREATER_EQUAL,
                        convertLocalDateTimeToTimestamp(start)));
    }

    @Override
    public LocalDateTime getNewestTimestamp(LocalDateTime previousNewestTime) throws KuduException {
        List<String> projectedColumns = new ArrayList<>();
        projectedColumns.add("time");

        Timestamp oldTime = convertLocalDateTimeToTimestamp(previousNewestTime);

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner scanner = scannerBuilder
                .setProjectedColumnNames(projectedColumns)
                .addPredicate(KuduPredicate.newComparisonPredicate(
                        kuduSchema.getColumn("time"),
                        KuduPredicate.ComparisonOp.GREATER,
                        oldTime
                ))
                .build();

        Timestamp newestTime = oldTime;
        while(scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while(results.hasNext()){
                RowResult result = results.next();
                Timestamp time = result.getTimestamp("time");
                if (time.after(newestTime)){
                    newestTime = time;
                }
            }
        }

        LocalDateTime newTime = newestTime.toLocalDateTime();
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
