package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.GeneratedEntry;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Writes the added entries to InfluxDB with nanosecond precision.
 */
public class InfluxTarget implements ITarget {
    private final InfluxDB influxDB;
    private final String measurementName;
    private boolean errorsOccurred;

    public InfluxTarget(String url, String username, String password, String dbName, String measurementName, boolean recreate, int maxBatchSize, int flushDuration) throws IOException {
        this.measurementName = measurementName;
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        if(influxDB.ping().getVersion().equalsIgnoreCase("unknown")) {
            influxDB.close();
            throw new IOException("No connection to Influx database.");
        }

        if(recreate){
            influxDB.query(new Query("DROP DATABASE " + dbName));
            influxDB.query(new Query("CREATE DATABASE " + dbName));
        }

        influxDB.setDatabase(dbName);
        influxDB.enableBatch(maxBatchSize, flushDuration, TimeUnit.MILLISECONDS, Executors.defaultThreadFactory(), (points, throwable) -> {
            errorsOccurred = true;
            assert false : "Error occurred during Influx insertion.";
        });
    }

    @Override
    public void add(GeneratedEntry entry) {
        Instant time = Instant.parse(entry.getTimestamp());
        long timeNano = time.getEpochSecond() * 1_000_000_000 + time.getNano();
        influxDB.write(
                Point.measurement(measurementName)
                        .time(timeNano, TimeUnit.NANOSECONDS)
                        .tag("AP", entry.getAP())
                        .addField("clients", entry.getNumClients())
                        .build());
    }

    @Override
    public boolean shouldStopEarly() {
        return errorsOccurred;
    }

    @Override
    public void close() {
        influxDB.disableBatch();
        influxDB.close();
    }
}
