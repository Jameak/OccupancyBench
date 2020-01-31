package Benchmark.Databases.Influx;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedColumnEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.HashMap;

/**
 * Writes the added entries to InfluxDB with nanosecond precision, in column-format.
 */
public class InfluxColumnTarget extends AbstractInfluxTarget {
    private final AccessPoint[] allAPs;

    public InfluxColumnTarget(ConfigFile config, boolean recreate, AccessPoint[] allAPs) throws IOException {
        super(config, recreate);
        this.allAPs = allAPs;
    }

    @Override
    public void add(IGeneratedEntry entry) {
        assert entry instanceof GeneratedColumnEntry : "Generated entry passed to column target must be a column-entry";
        GeneratedColumnEntry columnEntry = (GeneratedColumnEntry) entry;

        HashMap<String, Integer> map = columnEntry.getMapping();

        long time = columnEntry.getTime(granularity);
        Point.Builder builder = Point.measurement(measurementName).time(time, granularity.toTimeUnit());
        for(AccessPoint AP : allAPs){
            //Note: Influx doesn't support null as field-values so we are forced to write '0' here for non-existent APs.
            builder.addField(AP.getAPname(), map.getOrDefault(AP.getAPname(), 0));
        }
        influxDB.write(builder.build());
    }
}
