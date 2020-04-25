package Benchmark.Databases.Influx;

import Benchmark.Queries.IQueries;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Abstract class for Influx query-implementations containing default-implementations and convenience-functions.
 */
public abstract class AbstractInfluxQueries implements IQueries {
    protected InfluxDB influxDB;
    protected String measurement;

    @Override
    public LocalDateTime getNewestTimestamp(LocalDateTime previousNewestTime) {
        String queryString = String.format("SELECT * FROM %s WHERE time > %d ORDER BY time DESC LIMIT 1", measurement, toTimestamp(previousNewestTime));

        Query query = new Query(queryString);
        QueryResult results = influxDB.query(query);

        String time = null;
        for(QueryResult.Result result : results.getResults()){
            // Database doesn't exist, is empty, or the table is empty. May happen due to a race-condition between
            // ingestion and querying during startup caused by ingestion not having finished creating the table before
            // we try querying it, or ingestion may not have filled the first batch-insert yet to the empty table.
            // This error is rectified in the QueryRunnable-caller.
            if(result.getSeries() == null) continue;

            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    time = (String) entries.get(series.getColumns().indexOf("time"));
                }
            }
        }

        // If the database is empty, just return the old value
        if(time == null){
            return previousNewestTime;
        }

        return LocalDateTime.ofInstant(Instant.parse(time), ZoneOffset.ofHours(0));
    }

    @Override
    public void done() {
        influxDB.close();
    }

    protected long toTimestamp(LocalDateTime time){
        // Timestamps in Influx expect nanosecond precision
        return time.toEpochSecond(ZoneOffset.ofHours(0)) * 1_000_000_000;
    }

    protected double averageHistoricalClients(Map<String, List<Double>> results, String AP) {
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
}
