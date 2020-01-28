package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the benchmark-queries for InfluxDB.
 */
public class InfluxQueries implements Queries{
    private InfluxDB influxDB;
    private String measurement;
    private Map<Integer, String> precomputedFloorTotalQueryParts = new HashMap<>();
    private int sampleRate;

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception {
        this.measurement = config.getInfluxTable();
        this.sampleRate = config.getGeneratorGenerationInterval();
        // Some queries may take too long on my dev machine. If the connection timed out an exception will be thrown which stops the queries.
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder().readTimeout(60, TimeUnit.SECONDS);
        this.influxDB = InfluxDBFactory.connect(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword(), httpClientBuilder);
        if(influxDB.ping().getVersion().equalsIgnoreCase("unknown")) {
            influxDB.close();
            throw new IOException("No connection to Influx database.");
        }

        influxDB.setDatabase(config.getInfluxDBName());
        //NOTE: We cannot enable batching of these queries, because then we cant calculate how much time each query uses.

        //Pre-computations of the strings needed for the 'FloorTotal' query to minimize time spent in Java code versus on the query itself:
        for(Floor floor : generatedFloors){
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            boolean first = true;
            for(AccessPoint AP : floor.getAPs()){
                if(first){
                    first = false;
                } else {
                    sb.append("OR");
                }
                sb.append(" AP='");
                sb.append(AP.getAPname());
                sb.append("' ");
            }

            sb.append(") GROUP BY time(1d)");
            precomputedFloorTotalQueryParts.put(floor.getFloorNumber(), sb.toString());
        }
    }

    @Override
    public void done() {
        influxDB.close();
    }

    @Override
    public LocalDateTime getNewestTimestamp() {
        String queryString = String.format("SELECT * FROM %s ORDER BY time DESC LIMIT 1", measurement);

        Query query = new Query(queryString);
        QueryResult results = influxDB.query(query);

        String time = null;
        for(QueryResult.Result result : results.getResults()){
            assert result.getSeries() != null;
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    time = (String) entries.get(series.getColumns().indexOf("time"));
                }
            }
        }

        assert time != null;
        LocalDateTime dbTime = LocalDateTime.ofInstant(Instant.parse(time), ZoneOffset.ofHours(0));
        // Timescale and Influx seem to have weird behavior regarding exact matches on timestamp values, resulting in
        //   what seems to be off-by-one errors in the query-results.
        // To avoid this, we add a single second to the returned time to move slightly beyond the newest value.
        return dbTime.plusSeconds(1);
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) {
        List<Total> totals = new ArrayList<>();
        String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time < %d AND time >= %d GROUP BY time(1d)",
                measurement, toTimestamp(end), toTimestamp(start));

        Query query = new Query(queryString);
        influxDB.query(query);

        QueryResult results = influxDB.query(query);
        for(QueryResult.Result result : results.getResults()){
            if(result.getSeries() == null) continue; // No results. Caused by hole in data.
            assert result.getSeries().size() == 1;
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    String time = (String) entries.get(series.getColumns().indexOf("time"));
                    int total = (int)Math.round((Double)entries.get(series.getColumns().indexOf("sum")));

                    totals.add(new Total(time, total));
                }
            }
        }

        return totals;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) {
        List<FloorTotal> counts = new ArrayList<>(generatedFloors.length);

        for (Floor floor : generatedFloors) {
            String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time < %d AND time >= %d AND %s",
                    measurement, toTimestamp(end), toTimestamp(start), precomputedFloorTotalQueryParts.get(floor.getFloorNumber()));

            Query query = new Query(queryString);
            influxDB.query(query);
            QueryResult results = influxDB.query(query);
            for (QueryResult.Result result : results.getResults()) {
                if (result.getSeries() == null) continue; // No results. Caused by hole in data.
                assert result.getSeries().size() == 1;
                for (QueryResult.Series series : result.getSeries()) {
                    for (List<Object> entries : series.getValues()) {
                        int sum = (int) Math.round((Double) entries.get(series.getColumns().indexOf("sum")));
                        String timestamp = (String) entries.get(series.getColumns().indexOf("time"));
                        counts.add(new FloorTotal(floor.getFloorNumber(), timestamp, sum));
                    }
                }
            }
        }

        return counts;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) {
        List<MaxForAP> max = new ArrayList<>();

        String queryString = String.format("SELECT MAX(clients) FROM %s WHERE AP='%s' AND time < %d AND time >= %d GROUP BY time(1d)",
                measurement, AP.getAPname(), toTimestamp(end), toTimestamp(start));

        Query query = new Query(queryString);
        influxDB.query(query);

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
                        "WHERE time <= %s AND time > %s " +
                        "GROUP BY \"AP\"", measurement, toTimestamp(date), toTimestamp(date.minusMinutes(windowSizeInMin))));
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
                        "WHERE time <= %s AND time > %s " +
                        "GROUP BY \"AP\"", measurement, toTimestamp(date.plusMinutes(windowSizeInMin)), toTimestamp(date)));
                pastSoonStatQueries.add(query);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }

        Query query3 = new Query(String.format(
                "SELECT AP, clients AS client_entry " +
                "FROM %s " +
                "WHERE time <= %s AND time > %s ", measurement, toTimestamp(end), toTimestamp(end.minusSeconds(sampleRate))));

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
                    Integer clientEntry = (int)Math.round((Double) entries.get(series.getColumns().indexOf("client_entry")));
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

    private double averageHistoricalClients(Map<String, List<Double>> results, String AP) {
        double historicalClients;
        List<Double> queryResults = results.get(AP);
        int count = 0;
        Double total = 0.0;
        for(Double val : queryResults){
            count++;
            total += val;
        }
        assert count > 0 : "Cant divide by zero";
        historicalClients = total / count;
        return historicalClients;
    }

    private void parseAvgQuery(QueryResult queryResult, Map<String, List<Double>> target){
        for(QueryResult.Result result : queryResult.getResults()){
            if(result.getSeries() == null) continue; // There were no results at all for the query.
            for(QueryResult.Series series : result.getSeries()){
                String AP = series.getTags().get("AP");
                for(List<Object> entries : series.getValues()){
                    Double avgClients = (Double) entries.get(series.getColumns().indexOf("avg_clients"));
                    if(!target.containsKey(AP)){
                        target.put(AP, new ArrayList<>());
                    }

                    target.get(AP).add(avgClients);
                }
            }
        }
    }

    private long toTimestamp(LocalDateTime time){
        // Timestamps in Influx expect nanosecond precision
        return time.toEpochSecond(ZoneOffset.ofHours(0)) * 1_000_000_000;
    }
}
