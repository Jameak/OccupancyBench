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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the benchmark-queries for InfluxDB.
 */
public class InfluxQueries implements Queries{
    private InfluxDB influxDB;
    private ConfigFile config;
    private String measurement;
    private Map<Integer, String> precomputedFloorTotalQueryParts = new HashMap<>();

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception {
        this.measurement = config.getInfluxTable();
        // Some queries may take too long on my dev machine. If the connection timed out an exception will be thrown which stops the queries.
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder().readTimeout(60, TimeUnit.SECONDS);
        this.influxDB = InfluxDBFactory.connect(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword(), httpClientBuilder);
        this.config = config;
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
        return LocalDateTime.ofInstant(Instant.parse(time), ZoneOffset.ofHours(0));
    }

    @Override
    public int computeTotalClients(LocalDateTime start, LocalDateTime end) {
        int total = 0;
        String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time < %d AND time >= %d GROUP BY time(1d)",
                measurement, toTimestamp(end), toTimestamp(start));

        Query query = new Query(queryString);
        influxDB.query(query);
        //QueryResult results = influxDB.query(query);
        //for(QueryResult.Result result : results.getResults()){
        //    if(result.getSeries() == null) continue; // No results. Caused by hole in data.
        //    assert result.getSeries().size() == 1;
        //    for(QueryResult.Series series : result.getSeries()){
        //        for(List<Object> entries : series.getValues()){
        //            total = (int)Math.round((Double)entries.get(series.getColumns().indexOf("sum")));
        //        }
        //    }
        //}

        return total;
    }

    @Override
    public int[] computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) {
        int[] counts = new int[generatedFloors.length];
        for(int i = 0; i < generatedFloors.length; i++){
            Floor floor = generatedFloors[i];

            String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time < %d AND time >= %d AND %s",
                    measurement, toTimestamp(end), toTimestamp(start), precomputedFloorTotalQueryParts.get(floor.getFloorNumber()));

            Query query = new Query(queryString);
            influxDB.query(query);
            //QueryResult results = influxDB.query(query);
            //for(QueryResult.Result result : results.getResults()){
            //    if(result.getSeries() == null) continue; // No results. Caused by hole in data.
            //    assert result.getSeries().size() == 1;
            //    for(QueryResult.Series series : result.getSeries()){
            //        for(List<Object> entries : series.getValues()){
            //            counts[i] += (int)Math.round((Double)entries.get(series.getColumns().indexOf("sum")));
            //        }
            //    }
            //}
        }

        return counts;
    }

    @Override
    public int[] maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) {
        String queryString = String.format("SELECT MAX(clients) FROM %s WHERE AP='%s' AND time < %d AND time >= %d GROUP BY time(1d)",
                measurement, AP.getAPname(), toTimestamp(end), toTimestamp(start));

        Query query = new Query(queryString);
        influxDB.query(query);

        return new int[0];
    }

    private long toTimestamp(LocalDateTime time){
        return time.toEpochSecond(ZoneOffset.ofHours(0)) * 1_000_000_000;
    }
}
