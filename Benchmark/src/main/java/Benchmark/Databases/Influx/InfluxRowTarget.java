package Benchmark.Databases.Influx;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import org.influxdb.dto.Point;

import java.io.IOException;

/**
 * Writes the added entries to InfluxDB with nanosecond precision, in row-format.
 */
public class InfluxRowTarget extends AbstractInfluxTarget {
    public InfluxRowTarget(ConfigFile config, boolean recreate) throws IOException {
        super(config, recreate);
    }

    @Override
    public void add(IGeneratedEntry entry) {
        assert entry instanceof GeneratedRowEntry : "Generated entry passed to row target must be a row-entry";
        GeneratedRowEntry rowEntry = (GeneratedRowEntry) entry;

        long time = rowEntry.getTime(granularity);
        influxDB.write(
                Point.measurement(measurementName)
                        .time(time, granularity.toTimeUnit())
                        .tag("AP", rowEntry.getAP())
                        .addField("clients", rowEntry.getNumClients())
                        .build());
    }
}
