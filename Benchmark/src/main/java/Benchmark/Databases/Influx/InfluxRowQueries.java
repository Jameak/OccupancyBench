package Benchmark.Databases.Influx;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
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
 * An implementation of the benchmark-queries for InfluxDB when using the row-schema.
 */
public class InfluxRowQueries extends AbstractInfluxQueries {
    private Map<Integer, String> precomputedFloorTotalQueryParts = new HashMap<>();
    private int sampleRate;
    private GeneratedFloor[] generatedFloors;
    private Random rng;
    private GeneratedAccessPoint[] allAPs;

    @Override
    public void prepare(ConfigFile config, GeneratedFloor[] generatedFloors, Random rng) throws Exception {
        this.generatedFloors = generatedFloors;
        this.rng = rng;
        this.measurement = config.getInfluxTable();
        this.sampleRate = config.getGeneratorGenerationSamplerate();
        this.influxDB = InfluxHelper.openConnection(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword());
        this.allAPs = GeneratedFloor.allAPsOnFloors(generatedFloors);

        influxDB.setDatabase(config.getInfluxDBName());

        for(GeneratedFloor floor : generatedFloors){
            String precomp = QueryHelper.buildRowSchemaFloorTotalQueryPrecomputation(floor.getAPs());
            precomputedFloorTotalQueryParts.put(floor.getFloorNumber(), precomp);
        }
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) {
        List<Total> totals = new ArrayList<>();
        String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time > %d AND time <= %d GROUP BY time(1d)",
                measurement, toTimestamp(start), toTimestamp(end));

        Query query = new Query(queryString);
        QueryResult results = influxDB.query(query);
        for(QueryResult.Result result : results.getResults()){
            if(result.getSeries() == null) continue; // No results. Caused by hole in data.
            assert result.getSeries().size() == 1;
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    String time = (String) entries.get(series.getColumns().indexOf("time"));
                    Object sum = entries.get(series.getColumns().indexOf("sum"));
                    // No AP-data within the specified start/end interval for this day.
                    if(sum == null) continue;

                    int total = (int)Math.round((Double) sum);
                    totals.add(new Total(time, total));
                }
            }
        }

        return totals;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) {
        List<FloorTotal> counts = new ArrayList<>(generatedFloors.length);

        for (GeneratedFloor floor : generatedFloors) {
            String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time > %d AND time <= %d AND %s GROUP BY time(1d)",
                    measurement, toTimestamp(start), toTimestamp(end), precomputedFloorTotalQueryParts.get(floor.getFloorNumber()));

            Query query = new Query(queryString);
            QueryResult results = influxDB.query(query);
            for (QueryResult.Result result : results.getResults()) {
                if (result.getSeries() == null) continue; // No results. Caused by hole in data.
                assert result.getSeries().size() == 1;
                for (QueryResult.Series series : result.getSeries()) {
                    for (List<Object> entries : series.getValues()) {
                        String timestamp = (String) entries.get(series.getColumns().indexOf("time"));
                        Object sum = entries.get(series.getColumns().indexOf("sum"));
                        // No AP-data within the specified start/end interval for this day for this floor.
                        if(sum == null) continue;

                        int floorSum = (int) Math.round((Double) sum);
                        counts.add(new FloorTotal(floor.getFloorNumber(), timestamp, floorSum));
                    }
                }
            }
        }

        return counts;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, GeneratedAccessPoint AP) {
        List<MaxForAP> max = new ArrayList<>();

        String queryString = String.format("SELECT MAX(clients) FROM %s WHERE AP='%s' AND time > %d AND time <= %d GROUP BY time(1d)",
                measurement, AP.getAPname(), toTimestamp(start), toTimestamp(end));

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

                    // If the access point is missing from the data (e.g. because that particular AP crashed),
                    // then the returned value will be null.
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

    /**
     * Influx doesn't support joins, so we need to do application joins instead.
     * Additionally, Influx doesn't support OR in WHERE-clauses that deal with time, so instead of being able to do:
     *     SELECT ... FROM ... WHERE (time < TIME1 AND time > TIME2) OR (time < TIME3 AND time > TIME4)
     * we need to do one query for each of those time-intervals:
     *     SELECT ... FROM ... WHERE time < TIME1 AND time > TIME2
     *     SELECT ... FROM ... WHERE time < TIME3 AND time > TIME4
     */
    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) {
        List<Query> nowStatQueries = new ArrayList<>();
        List<Query> pastSoonStatQueries = new ArrayList<>();

        {
            LocalDateTime date = end;
            do{
                Query query = new Query(String.format(
                        "SELECT MEAN(clients) AS avg_clients " +
                        "FROM %s " +
                        "WHERE time > %s AND time <= %s " +
                        "GROUP BY \"AP\"", measurement, toTimestamp(date.minusMinutes(windowSizeInMin)), toTimestamp(date)));
                nowStatQueries.add(query);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }
        {
            LocalDateTime date = end;
            do{
                Query query = new Query(String.format(
                        "SELECT MEAN(clients) AS avg_clients " +
                        "FROM %s " +
                        "WHERE time > %s AND time <= %s " +
                        "GROUP BY \"AP\"", measurement, toTimestamp(date), toTimestamp(date.plusMinutes(windowSizeInMin))));
                pastSoonStatQueries.add(query);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }

        Query query3 = new Query(String.format(
                "SELECT AP, clients AS client_entry " +
                "FROM %s " +
                "WHERE time > %s AND time <= %s ", measurement, toTimestamp(end.minusSeconds(sampleRate)), toTimestamp(end)));

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
                    String AP = (String) entries.get(series.getColumns().indexOf("AP"));
                    Object retry = entries.get(series.getColumns().indexOf("client_entry"));
                    if(retry == null) continue; // No data exists that fits our search criteria.

                    Integer clientEntry = (int)Math.round((Double) retry);
                    parsedQ3.put(AP, clientEntry);
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
                String AP = series.getTags().get("AP");
                for(List<Object> entries : series.getValues()){
                    Object avg = entries.get(series.getColumns().indexOf("avg_clients"));
                    if(avg == null) continue; // No data exists that fits our search criteria.

                    Double avgClients = (Double) avg;
                    if(!target.containsKey(AP)){
                        target.put(AP, new ArrayList<>());
                    }

                    target.get(AP).add(avgClients);
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
            String queryString = String.format("SELECT clients FROM %s WHERE AP='%s' AND time > %d AND time <= %d ORDER BY time ASC",
                    measurement, AP, toTimestamp(start), toTimestamp(end));

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
                        Object clientsValue = entries.get(series.getColumns().indexOf("clients"));

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
