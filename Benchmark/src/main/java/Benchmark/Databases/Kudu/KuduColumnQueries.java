package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Queries.Results.*;
import org.apache.kudu.client.*;

import java.time.LocalDateTime;
import java.util.*;

public class KuduColumnQueries extends AbstractKuduQueries {
    private ConfigFile config;
    private Floor[] generatedFloors;
    private int sampleRate;
    private KuduSession kuduSession;
    private HashMap<Integer, List<String>> floorAPs;
    private HashMap<Integer, List<String>> floorColumns;
    private List<String> allAPs;
    private List<String> allColumns;

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors, Random rng) throws KuduException {
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
        floorColumns = new HashMap<>();
        for(Floor floor : generatedFloors){
            List<String> columns = new ArrayList<>();
            List<String> APs = new ArrayList<>();
            columns.add("time");
            for(AccessPoint AP : floor.getAPs()){
                APs.add(AP.getAPname());
            }
            columns.addAll(APs);
            floorAPs.put(floor.getFloorNumber(), APs);
            floorColumns.put(floor.getFloorNumber(), columns);
        }

        allAPs = new ArrayList<>();
        allColumns = new ArrayList<>();
        allColumns.add("time");
        for(Floor floor : generatedFloors){
            for(AccessPoint AP : floor.getAPs()){
                allAPs.add(AP.getAPname());
            }
        }
        allColumns.addAll(allAPs);
    }

    @Override
    public void done() throws KuduException {
        kuduSession.close();
        kuduClient.close();
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws KuduException {
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner scanner = addTimeComparisonPredicates(scannerBuilder, start, end)
                .setProjectedColumnNames(allColumns)
                .build();

        List<Total> totals = new ArrayList<>();

        groupByDayAndCompute(start, end, scanner, (int total, RowResult result) -> {
            for(String AP : allAPs){
                total += result.getInt(AP);
            }
            return total;
        }, (timestamp, queryResult) -> totals.add(new Total(timestamp, queryResult)));

        scanner.close();
        return totals;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) throws KuduException {
        List<FloorTotal> floorTotals = new ArrayList<>();

        for(Floor floor : generatedFloors){
            KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
            KuduScanner scanner = addTimeComparisonPredicates(scannerBuilder, start, end)
                    .setProjectedColumnNames(floorColumns.get(floor.getFloorNumber()))
                    .build();

            groupByDayAndCompute(start, end, scanner, (int total, RowResult result) -> {
                for(String AP : floorAPs.get(floor.getFloorNumber())){
                    total += result.getInt(AP);
                }
                return total;
            }, (timestamp, queryResult) -> floorTotals.add(new FloorTotal(floor.getFloorNumber(), timestamp, queryResult)));

            scanner.close();
        }

        return floorTotals;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws KuduException {
        List<MaxForAP> max = new ArrayList<>();

        List<String> projectedColumns = new ArrayList<>();
        projectedColumns.add("time");
        projectedColumns.add(AP.getAPname());

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner scanner = addTimeComparisonPredicates(scannerBuilder, start, end)
                .setProjectedColumnNames(projectedColumns)
                .build();

        groupByDayAndCompute(start, end, scanner, (prevValue, result) -> Math.max(prevValue, result.getInt(AP.getAPname())),
                (timestamp, queryResult) -> max.add(new MaxForAP(AP.getAPname(), timestamp, queryResult)));

        scanner.close();
        return max;
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws KuduException {
        return computeAvgOccupancyGivenProjectedColumns(start, end, windowSizeInMin, sampleRate, allAPs);
    }

    @Override
    protected void scanAndHandle(KuduScanner scanner, AbstractKuduQueries.AvgHandler handler) throws KuduException {
        while(scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while(results.hasNext()){
                RowResult result = results.next();
                for(String AP : allAPs){
                    int val = result.getInt(AP);
                    handler.run(val, AP);
                }
            }
        }
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws KuduException {
        return null;
    }
}
