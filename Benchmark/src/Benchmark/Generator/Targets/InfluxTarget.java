package Benchmark.Generator.Targets;

import Benchmark.Generator.DataGenerator;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class InfluxTarget implements ITarget {
    private final InfluxDB influxDB;
    private final String measurementName;

    public InfluxTarget(String url, String username, String password, String dbName, String measurementName) throws IOException {
        this.measurementName = measurementName;
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        if(influxDB.ping().getVersion().equalsIgnoreCase("unknown")) {
            influxDB.close();
            throw new IOException("No connection to Influx database.");
        }

        //TODO: Check if database exists and create it if it doesn't?
        //  And if it already exists then drop it first?

        influxDB.setDatabase(dbName);
        influxDB.enableBatch(BatchOptions.DEFAULTS); //TODO: Batching seems to force asynchronous error handling. Currently I just ignore any errors that might happen. I should handle them explicitly?
    }

    @Override
    public void add(DataGenerator.GeneratedEntry entry) throws IOException {
        Instant time = Instant.parse(entry.getTimestamp());
        long timeNano = time.getEpochSecond() * 1_000_000_000 + time.getNano();
        influxDB.write(
                Point.measurement(measurementName)
                        .time(timeNano, TimeUnit.NANOSECONDS)
                        .addField("AP", entry.getAP())
                        .addField("clients", entry.getNumClients())
                        .build());
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
    }
}
