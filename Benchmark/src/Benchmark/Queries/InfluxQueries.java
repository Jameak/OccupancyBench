package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.AccessPoint;
import Benchmark.Generator.Floor;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class InfluxQueries implements Queries{
    private InfluxDB influxDB;
    private ConfigFile config;
    private String measurement;

    @Override
    public void prepare(ConfigFile config) throws Exception {
        this.measurement = config.influxTable();
        this.influxDB = InfluxDBFactory.connect(config.influxUrl(), config.influxUsername(), config.influxPassword());
        this.config = config;
        if(influxDB.ping().getVersion().equalsIgnoreCase("unknown")) {
            influxDB.close();
            throw new IOException("No connection to Influx database.");
        }

        influxDB.setDatabase(config.influxDBName());
        //NOTE: Cannot enable batching of these queries, because then we cant calculate how much time each query uses.
    }

    @Override
    public void done() {
        influxDB.close();
    }

    @Override
    public int computeTotalClients(LocalDateTime start, LocalDateTime end) {
        int total = 0;
        // How do we define the start/end span for points to include at an arbitrary point in time?
        //   This is the naive option (which is probably fine for the benchmark) but has a small risk of APs appearing more than once.
        String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time < %d AND time > %d GROUP BY time(%ss)",
                measurement, toTimestamp(end), toTimestamp(start), config.generationinterval());

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

            // How do we define the start/end span for points to include at an arbitrary point in time?
            //   This is the naive option (which is probably fine for the benchmark) but has a small risk of APs appearing more than once.
            String queryString = String.format("SELECT SUM(clients) FROM %s WHERE time < %d AND time > %d",
                    measurement, toTimestamp(end), toTimestamp(start));

            StringBuilder sb = new StringBuilder(queryString);
            sb.append(" AND (");
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

            sb.append(") GROUP BY time(");
            sb.append(config.generationinterval());
            sb.append("s)");

            Query query = new Query(sb.toString());
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

    private long toTimestamp(LocalDateTime time){
        return time.toEpochSecond(ZoneOffset.ofHours(0)) * 1_000_000_000;
    }
}
