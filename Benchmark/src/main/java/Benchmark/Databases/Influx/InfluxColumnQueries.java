package Benchmark.Databases.Influx;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Queries.KMeansImplementation;
import Benchmark.Queries.QueryHelper;
import Benchmark.Queries.Results.*;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * An implementation of the benchmark-queries for InfluxDB when using the column-schema.
 */
public class InfluxColumnQueries extends AbstractInfluxQueries {
    private AccessPoint[] allAPs;
    private int sampleRate;
    private String precomputedTotalClientsPart;
    private String precomputedFloorTotalPart;
    private String precomputedAvgOccupancyPart1;
    private String precomputedAvgOccupancyPart2;
    private Floor[] generatedFloors;
    private Random rng;

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors, Random rng) throws Exception {
        this.generatedFloors = generatedFloors;
        this.rng = rng;
        this.measurement = config.getInfluxTable();
        this.sampleRate = config.getGeneratorGenerationInterval();
        this.influxDB = InfluxHelper.openConnection(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword());
        this.allAPs = Floor.allAPsOnFloors(generatedFloors);

        influxDB.setDatabase(config.getInfluxDBName());
        //NOTE: We cannot enable batching of these queries, because then we cant calculate how much time each query uses.

        precomputedTotalClientsPart = QueryHelper.buildColumnSchemaTotalClientsQueryPrecomputation(allAPs);
        precomputedFloorTotalPart = QueryHelper.buildColumnSchemaFloorTotalQueryPrecomputation(generatedFloors);
        precomputedAvgOccupancyPart1 = QueryHelper.buildColumnSchemaAvgOccupancyPrecomputation_AVG("MEAN", allAPs);
        precomputedAvgOccupancyPart2 = QueryHelper.buildColumnSchemaAvgOccupancyPrecomputation_SELECT_ALL(allAPs);
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) {
        List<Total> totals = new ArrayList<>();

        String queryString = String.format("SELECT %s FROM %s WHERE time > %d AND time <= %d GROUP BY time(1d)",
                precomputedTotalClientsPart, measurement, toTimestamp(start), toTimestamp(end));

        Query query = new Query(queryString);
        QueryResult results = influxDB.query(query);
        for(QueryResult.Result result : results.getResults()){
            if(result.getSeries() == null) continue; // No results. Caused by hole in data.
            assert result.getSeries().size() == 1;
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    String time = (String) entries.get(series.getColumns().indexOf("time"));
                    int total = (int)Math.round((Double)entries.get(series.getColumns().indexOf("total")));

                    totals.add(new Total(time, total));
                }
            }
        }

        return totals;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) {
        List<FloorTotal> counts = new ArrayList<>(generatedFloors.length);

        String queryString = String.format("SELECT %s FROM %s WHERE time > %d AND time <= %d GROUP BY time(1d)",
                precomputedFloorTotalPart, measurement, toTimestamp(start), toTimestamp(end));

        Query query = new Query(queryString);
        QueryResult results = influxDB.query(query);
        for (QueryResult.Result result : results.getResults()) {
            if (result.getSeries() == null) continue; // No results. Caused by hole in data.
            assert result.getSeries().size() == 1;
            for (QueryResult.Series series : result.getSeries()) {
                for (List<Object> entries : series.getValues()) {
                    String timestamp = (String) entries.get(series.getColumns().indexOf("time"));
                    for(Floor floor : generatedFloors){
                        int floorSum = (int) Math.round((Double) entries.get(series.getColumns().indexOf("floor" + floor.getFloorNumber())));
                        counts.add(new FloorTotal(floor.getFloorNumber(), timestamp, floorSum));
                    }
                }
            }
        }

        return counts;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) {
        List<MaxForAP> max = new ArrayList<>();

        String queryString = String.format("SELECT MAX(\"%s\") FROM %s WHERE time > %d AND time <= %d GROUP BY time(1d)",
                AP.getAPname(), measurement, toTimestamp(start), toTimestamp(end));

        Query query = new Query(queryString);
        QueryResult results = influxDB.query(query);
        for(QueryResult.Result result : results.getResults()){
            if(result.getSeries() == null) continue; // No results. Caused by hole in data.
            assert result.getSeries().size() == 1;
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    String time = (String) entries.get(series.getColumns().indexOf("time"));
                    int maxIndex = series.getColumns().indexOf("max");
                    Object maxValue = entries.get(maxIndex);

                    // AP had no values on day within our time-interval.
                    if(maxValue == null){
                        continue;
                    }

                    int maxVal = (int)Math.round((Double)maxValue);
                    max.add(new MaxForAP(AP.getAPname(), time, maxVal));
                }
            }
        }

        return max;
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) {
        List<Query> nowStatQueries = new ArrayList<>();
        List<Query> pastSoonStatQueries = new ArrayList<>();

        {
            LocalDateTime date = end;
            do{
                Query query = new Query(String.format(
                        "SELECT %s " +
                        "FROM %s " +
                        "WHERE time > %s AND time <= %s "
                        , precomputedAvgOccupancyPart1, measurement, toTimestamp(date.minusMinutes(windowSizeInMin)), toTimestamp(date)));
                nowStatQueries.add(query);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }
        {
            LocalDateTime date = end;
            do{
                Query query = new Query(String.format(
                        "SELECT %s " +
                        "FROM %s " +
                        "WHERE time > %s AND time <= %s "
                        , precomputedAvgOccupancyPart1, measurement, toTimestamp(date), toTimestamp(date.plusMinutes(windowSizeInMin))));
                pastSoonStatQueries.add(query);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }

        Query query3 = new Query(String.format(
                "SELECT %s " +
                "FROM %s " +
                "WHERE time > %s AND time <= %s " +
                "LIMIT 1" // LIMIT shouldn't technically be needed since the time-intervals should limit the interval to just 1 entry.
                , precomputedAvgOccupancyPart2, measurement, toTimestamp(end.minusSeconds(sampleRate)), toTimestamp(end)));

        List<QueryResult> resQ1 = new ArrayList<>();
        for(Query query : nowStatQueries){
            resQ1.add(influxDB.query(query));
        }
        List<QueryResult> resQ2 = new ArrayList<>();
        for(Query query : pastSoonStatQueries){
            resQ2.add(influxDB.query(query));
        }
        QueryResult resQ3 = influxDB.query(query3);

        Map<String, List<Double>> parsedQ1 = new HashMap<>();
        Map<String, List<Double>> parsedQ2 = new HashMap<>();
        Map<String, Integer> parsedQ3 = new HashMap<>();

        for(QueryResult resultQ1 : resQ1){
            parseAvgQuery(resultQ1, parsedQ1);
        }
        for(QueryResult resultQ2 : resQ2){
            parseAvgQuery(resultQ2, parsedQ2);
        }

        for(QueryResult.Result result : resQ3.getResults()){
            if(result.getSeries() == null) continue; // There were no results at all for the query.
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    for(AccessPoint AP : allAPs){
                        Integer numClients = (int)Math.round((Double) entries.get(series.getColumns().indexOf(AP.getAPname())));
                        parsedQ3.put(AP.getAPname(), numClients);
                    }
                }
            }
        }

        List<AvgOccupancy> output = new ArrayList<>();
        for(String AP : parsedQ3.keySet()){
            if(!(parsedQ1.containsKey(AP) && parsedQ2.containsKey(AP))){
                //We're modeling an inner join, so Q1, Q2 and Q3 must contain the AP.
                continue;
            }

            int currentClients = parsedQ3.get(AP);
            double historicalClientsNow = averageHistoricalClients(parsedQ1, AP);
            double historicalClientsSoon = averageHistoricalClients(parsedQ2, AP);

            output.add(new AvgOccupancy(AP, currentClients, historicalClientsNow, historicalClientsSoon));
        }

        return output;
    }

    private void parseAvgQuery(QueryResult queryResult, Map<String, List<Double>> target){
        for(QueryResult.Result result : queryResult.getResults()){
            if(result.getSeries() == null) continue; // There were no results at all for the query.
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    for(AccessPoint AP : allAPs){
                        Double avgClients = (Double) entries.get(series.getColumns().indexOf("avg-" + AP.getAPname()));
                        if(!target.containsKey(AP.getAPname())){
                            target.put(AP.getAPname(), new ArrayList<>());
                        }

                        target.get(AP.getAPname()).add(avgClients);
                    }
                }
            }
        }
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws IOException, SQLException {
        long secondsInInterval = start.until(end, ChronoUnit.SECONDS);
        //Estimate the number of entries we'll get. We'll probably get slightly less than this number due to outages from the seed data.
        int numEntries = Math.toIntExact(secondsInInterval / sampleRate);

        KMeansImplementation kmeans = new KMeansImplementation(numIterations, numClusters, allAPs, rng, AP -> {
            String queryString = String.format("SELECT \"%s\" FROM %s WHERE time > %d AND time <= %d ORDER BY time ASC",
                    AP, measurement, toTimestamp(start), toTimestamp(end));

            Instant[] timestamps = new Instant[numEntries];
            int[] values = new int[numEntries];
            int index = 0;

            Query query = new Query(queryString);
            QueryResult results = influxDB.query(query);
            for(QueryResult.Result result : results.getResults()){
                if(result.getSeries() == null) continue; // No results. Caused by hole in data.
                assert result.getSeries().size() == 1;
                for(QueryResult.Series series : result.getSeries()){
                    for(List<Object> entries : series.getValues()){
                        if(index == numEntries) break;

                        String time = (String) entries.get(series.getColumns().indexOf("time"));
                        Object clientsValue = entries.get(series.getColumns().indexOf(AP));

                        // For a 'good' K-Means implementation, what should we do when values are missing?
                        if(clientsValue == null){
                            index++;
                            continue;
                        }

                        int clients = (int)Math.round((Double)clientsValue);
                        timestamps[index] = Instant.parse(time);
                        values[index] = clients;
                        index++;
                    }
                }
            }

            return new KMeansImplementation.TimeSeries(timestamps, values);
        });

        return kmeans.computeKMeans();
    }
}
