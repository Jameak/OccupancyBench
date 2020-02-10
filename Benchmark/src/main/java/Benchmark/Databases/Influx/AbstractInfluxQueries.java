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
