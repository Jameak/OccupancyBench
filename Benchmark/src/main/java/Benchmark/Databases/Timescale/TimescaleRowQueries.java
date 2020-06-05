package Benchmark.Databases.Timescale;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.Queries.KMeansImplementation;
import Benchmark.Queries.QueryHelper;
import Benchmark.Queries.Results.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * An implementation of the benchmark-queries for TimescaleDB when using the row-schema.
 */
public class TimescaleRowQueries extends AbstractTimescaleQueries {
    private int sampleRate;
    private Map<Integer, String> precomputedFloorTotalQueryParts = new HashMap<>();
    private GeneratedFloor[] generatedFloors;
    private Random rng;
    private GeneratedAccessPoint[] allAPs;

    @Override
    public void prepare(ConfigFile config, GeneratedFloor[] generatedFloors, Random rng) throws Exception {
        this.generatedFloors = generatedFloors;
        this.rng = rng;
        this.table = config.getTimescaleTable();
        this.sampleRate = config.getGeneratorGenerationSamplerate();
        this.connection = TimescaleHelper.openConnection(config.getTimescaleUsername(), config.getTimescalePassword(), config.getTimescaleHost(), config.getTimescaleDBName(), false);
        this.allAPs = GeneratedFloor.allAPsOnFloors(generatedFloors);

        for(GeneratedFloor floor : generatedFloors){
            String precomp = QueryHelper.buildRowSchemaFloorTotalQueryPrecomputation(floor.getAPs());
            precomputedFloorTotalQueryParts.put(floor.getFloorNumber(), precomp);
        }
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);
        List<Total> totals = new ArrayList<>();

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, SUM(clients) " +
                "FROM %s " +
                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", table, timeStart, timeEnd);

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                String time = results.getString("bucket");
                int total = results.getInt("sum");
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

        for (GeneratedFloor floor : generatedFloors) {
            String query = String.format("SELECT time_bucket('1 day', time) AS bucket, SUM(clients) " +
                            "FROM %s " +
                            "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) AND %s " +
                            "GROUP BY bucket",
                    table, timeStart, timeEnd, precomputedFloorTotalQueryParts.get(floor.getFloorNumber()));

            try (Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery(query)) {
                 while (results.next()) {
                    String time = results.getString("bucket");
                    int total = results.getInt("sum");
                    floorTotals.add(new FloorTotal(floor.getFloorNumber(), time, total));
                 }
            }
        }

        return floorTotals;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, GeneratedAccessPoint AP) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);
        List<MaxForAP> max = new ArrayList<>();

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, MAX(clients) " +
                "FROM %s " +
                "WHERE AP='%s' AND time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", table, AP.getAPname(), timeStart, timeEnd);

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

        String query = String.format(
                "SELECT now.AP, " +
                        "FIRST(now.client_entry, now.time) AS current_clients, " +
                        "AVG(now_stat.avg_clients) AS historical_clients_now, " +
                        "AVG(past_soon.avg_clients) AS historical_clients_soon " +
                "FROM (" +
                        "SELECT AP, AVG(clients) AS avg_clients " +
                        "FROM %s " +
                        "WHERE %s " +
                        "GROUP BY AP " +
                ") AS now_stat " +
                "INNER JOIN (" +
                        "SELECT AP, AVG(clients) as avg_clients " +
                        "FROM %s " +
                        "WHERE %s " +
                        "GROUP BY AP " +
                ") AS past_soon " +
                "ON now_stat.ap = past_soon.ap " +
                "INNER JOIN (" +
                        "SELECT AP, clients as client_entry, time " +
                        "FROM %s " +
                        "WHERE time <= TO_TIMESTAMP(%s) AND time > TO_TIMESTAMP(%s) " +
                ") AS now " +
                "ON now.ap = now_stat.ap " +
                "GROUP BY now.AP", table, sb1.toString(), table, sb2.toString(), table, toTimestamp(end), toTimestamp(end.minusSeconds(sampleRate)));

        List<AvgOccupancy> output = new ArrayList<>();
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                String AP = results.getString("AP");
                int currentClients = results.getInt("current_clients");
                double historicalClientsNow = results.getDouble("historical_clients_now");
                double historicalClientsSoon = results.getDouble("historical_clients_soon");
                output.add(new AvgOccupancy(AP, currentClients, historicalClientsNow, historicalClientsSoon));
            }
        }

        return output;
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws SQLException, IOException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);
        long secondsInInterval = start.until(end, ChronoUnit.SECONDS);
        //Estimate the number of entries we'll get. We'll probably get slightly less than this number due to outages from the seed data.
        int numEntries = Math.toIntExact(secondsInInterval / sampleRate);

        KMeansImplementation kmeans = new KMeansImplementation(numIterations, numClusters, allAPs, rng, AP -> {
            String query = String.format("SELECT time, clients " +
                    "FROM %s " +
                    "WHERE AP='%s' AND time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                    "ORDER BY time ASC",
                    table, AP, timeStart, timeEnd);

            Instant[] timestamps = new Instant[numEntries];
            int[] values = new int[numEntries];
            int index = 0;

            try(Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(query)){
                while(results.next()) {
                    if(index == numEntries) break;

                    Timestamp time = results.getTimestamp("time");
                    int clients = results.getInt("clients");

                    timestamps[index] = time.toInstant();
                    values[index] = clients;
                    index++;
                }
            }

            return new KMeansImplementation.TimeSeries(timestamps, values);
        });

        return kmeans.computeKMeans();
    }
}
