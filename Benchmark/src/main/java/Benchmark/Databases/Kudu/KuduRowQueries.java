package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Queries.Results.*;
import org.apache.kudu.client.*;

import java.time.LocalDateTime;
import java.util.*;

public class KuduRowQueries extends AbstractKuduQueries {
    private ConfigFile config;
    private Floor[] generatedFloors;
    private int sampleRate;
    private KuduSession kuduSession;
    private HashMap<Integer, List<String>> floorAPs;

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors, Random rng) throws Exception {
        this.config = config;
        this.generatedFloors = generatedFloors;
        this.sampleRate = config.getGeneratorGenerationInterval();
        this.kuduClient = KuduHelper.openConnection(config);
        this.kuduTable = kuduClient.openTable(config.getKuduTable());
        this.kuduSession = kuduClient.newSession();
        this.kuduSchema = kuduTable.getSchema();
        // Makes 'apply' synchronous. No batching will occur.
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        floorAPs = new HashMap<>();
        for(Floor floor : generatedFloors){
            List<String> APs = new ArrayList<>();
            for(AccessPoint AP : floor.getAPs()){
                APs.add(AP.getAPname());
            }
            floorAPs.put(floor.getFloorNumber(), APs);
        }
    }

    @Override
    public void done() throws KuduException {
        kuduSession.close();
        kuduClient.close();
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws KuduException {
        List<String> projectedColumns = new ArrayList<>();
        projectedColumns.add("time");
        projectedColumns.add("clients");

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner scanner = addTimeComparisonPredicates(scannerBuilder, start, end)
                .setProjectedColumnNames(projectedColumns)
                .build();

        List<Total> totals = new ArrayList<>();

        groupByDayAndCompute(start, end, scanner, (previousValue, result) -> previousValue + result.getInt("clients"),
                (timestamp, queryResult) -> totals.add(new Total(timestamp, queryResult)));

        scanner.close();
        return totals;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) throws KuduException {
        List<FloorTotal> floorTotals = new ArrayList<>();

        List<String> projectedColumns = new ArrayList<>();
        projectedColumns.add("time");
        projectedColumns.add("clients");

        for(Floor floor : generatedFloors){
            KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
            KuduScanner scanner = addTimeComparisonPredicates(scannerBuilder, start, end)
                    .setProjectedColumnNames(projectedColumns)
                    .addPredicate(KuduPredicate.newInListPredicate(
                            kuduSchema.getColumn("AP"),
                            floorAPs.get(floor.getFloorNumber())))
                    .build();

            groupByDayAndCompute(start, end, scanner, (previousValue, result) -> previousValue + result.getInt("clients"),
                    (timestamp, queryResult) -> floorTotals.add(new FloorTotal(floor.getFloorNumber(), timestamp, queryResult)));

            scanner.close();
        }

        return floorTotals;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws KuduException {
        List<MaxForAP> max = new ArrayList<>();

        List<String> projectedColumns = new ArrayList<>();
        projectedColumns.add("time");
        projectedColumns.add("clients");
        List<String> apToGet = new ArrayList<>();
        apToGet.add(AP.getAPname());

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner scanner = addTimeComparisonPredicates(scannerBuilder, start, end)
                .setProjectedColumnNames(projectedColumns)
                .addPredicate(KuduPredicate.newInListPredicate(
                        kuduSchema.getColumn("AP"),
                        apToGet // The Kudu library implementation specializes this predicate to an equality check since the list only has 1 value.
                ))
                .build();

        groupByDayAndCompute(start, end, scanner, (previousValue, result) ->  Math.max(previousValue, result.getInt("clients")),
                (timestamp, queryResult) -> max.add(new MaxForAP(AP.getAPname(), timestamp, queryResult)));

        scanner.close();
        return max;
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws KuduException {
        List<String> projectedColumns = new ArrayList<>(2);
        projectedColumns.add("AP");
        projectedColumns.add("clients");

        return computeAvgOccupancyGivenProjectedColumns(start, end, windowSizeInMin, sampleRate, projectedColumns);
    }

    @Override
    protected void scanAndHandle(KuduScanner scanner, AbstractKuduQueries.AvgHandler handler) throws KuduException {
        while(scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while(results.hasNext()){
                RowResult result = results.next();
                String AP = result.getString("AP");
                int value = result.getInt("clients");
                handler.run(value, AP);
            }
        }
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws KuduException {
        return null;
    }
}
