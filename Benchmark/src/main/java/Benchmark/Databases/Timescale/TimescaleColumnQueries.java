package Benchmark.Databases.Timescale;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Queries.QueryHelper;
import Benchmark.Queries.Results.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;

/**
 * An implementation of the benchmark-queries for TimescaleDB when using the column-schema.
 */
public class TimescaleColumnQueries extends AbstractTimescaleQueries {
    private String precomputedTotalClientsPart;
    private String precomputedFloorTotalPart;
    private String precomputedAvgOccupancyPart1;
    private String precomputedAvgOccupancyPart2;
    private AccessPoint[] allAPs;
    private int sampleRate;
    private Floor[] generatedFloors;

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors, Random rng) throws Exception {
        this.generatedFloors = generatedFloors;
        this.table = config.getTimescaleTable();
        this.sampleRate = config.getGeneratorGenerationInterval();
        this.connection = TimescaleHelper.openConnection(config.getTimescaleUsername(), config.getTimescalePassword(), config.getTimescaleHost(), config.getTimescaleDBName(), false);
        this.allAPs = Floor.allAPsOnFloors(generatedFloors);

        precomputedTotalClientsPart = QueryHelper.buildColumnSchemaTotalClientsQueryPrecomputation(allAPs);
        precomputedFloorTotalPart = QueryHelper.buildColumnSchemaFloorTotalQueryPrecomputation(generatedFloors);
        precomputedAvgOccupancyPart1 = QueryHelper.buildColumnSchemaAvgOccupancyPrecomputation_AVG("AVG", allAPs);
        precomputedAvgOccupancyPart2 = QueryHelper.buildColumnSchemaAvgOccupancyPrecomputation_SELECT_ALL(allAPs);
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);
        List<Total> totals = new ArrayList<>();

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, %s " +
                "FROM %s " +
                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", precomputedTotalClientsPart, table, timeStart, timeEnd);

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                String time = results.getString("bucket");
                int total = results.getInt("total");
                totals.add(new Total(time, total));
            }
        }

        return totals;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);
        List<FloorTotal> floorTotals = new ArrayList<>();

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, %s " +
                        "FROM %s " +
                        "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s)" +
                        "GROUP BY bucket",
                        precomputedFloorTotalPart, table, timeStart, timeEnd);

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(query)) {
             while (results.next()) {
                String time = results.getString("bucket");
                for(Floor floor : generatedFloors){
                    int floorTotal = results.getInt("floor" + floor.getFloorNumber());
                    floorTotals.add(new FloorTotal(floor.getFloorNumber(), time, floorTotal));
                }
             }
        }

        return floorTotals;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);
        List<MaxForAP> max = new ArrayList<>();

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, MAX(\"%s\") " +
                "FROM %s " +
                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", AP.getAPname(), table, timeStart, timeEnd);

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                String time = results.getString("bucket");
                int maxVal = results.getInt("max");
                max.add(new MaxForAP(AP.getAPname(), time, maxVal));
            }
        }

        return max;
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws SQLException {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        buildAvgOccupancyTimeranges(start, end, sb1, sb2, windowSizeInMin);

        String query1 = String.format(
                "SELECT %s " +
                "FROM %s " +
                "WHERE %s",
                precomputedAvgOccupancyPart1, table, sb1.toString());

        String query2 = String.format(
                "SELECT %s " +
                "FROM %s " +
                "WHERE %s",
                precomputedAvgOccupancyPart1, table, sb2.toString());

        String query3 = String.format(
                "SELECT %s " +
                "FROM %s " +
                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s)",
                precomputedAvgOccupancyPart2, table, toTimestamp(end.minusSeconds(sampleRate)), toTimestamp(end));

        Map<String, Double> parsedQ1 = new HashMap<>();
        Map<String, Double> parsedQ2 = new HashMap<>();
        Map<String, Integer> parsedQ3 = new HashMap<>();

        try(Statement statement = connection.createStatement()){
            try(ResultSet results = statement.executeQuery(query1)){
                while(results.next()) {
                    for(AccessPoint AP : allAPs){
                        // Note: ResultSet.getDouble automatically converts NULL to 0 so we cant tell the difference
                        //       between a missing AP-value and a normal 0
                        double historicalClientsNow = results.getDouble("avg-" + AP.getAPname());
                        parsedQ1.put(AP.getAPname(), historicalClientsNow);
                    }
                }
            }

            try(ResultSet results = statement.executeQuery(query2)){
                while(results.next()) {
                    for(AccessPoint AP : allAPs){
                        double historicalClientsSoon = results.getDouble("avg-" + AP.getAPname());
                        parsedQ2.put(AP.getAPname(), historicalClientsSoon);
                    }
                }
            }

            try(ResultSet results = statement.executeQuery(query3)){
                while(results.next()) {
                    for(AccessPoint AP : allAPs){
                        int currentClients = results.getInt(AP.getAPname());
                        parsedQ3.put(AP.getAPname(), currentClients);
                    }
                }
            }
        }

        List<AvgOccupancy> output = new ArrayList<>();
        for(AccessPoint AP : allAPs){
            String name = AP.getAPname();
            assert parsedQ1.containsKey(name) && parsedQ2.containsKey(name) && parsedQ3.containsKey(name);

            output.add(new AvgOccupancy(name, parsedQ3.get(name), parsedQ1.get(name), parsedQ2.get(name)));
        }

        return output;
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws SQLException {
        return null;
    }
}
