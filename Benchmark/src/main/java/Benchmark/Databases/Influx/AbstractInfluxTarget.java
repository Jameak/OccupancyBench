package Benchmark.Databases.Influx;

import Benchmark.Config.ConfigFile;
import Benchmark.Config.Granularity;
import Benchmark.Generator.Targets.ITarget;
import org.influxdb.InfluxDB;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for Influx target-implementations containing default-implementation for some ITarget-functions and
 * a constructor that handles the database-setup that any Influx-implementation must do.
 */
public abstract class AbstractInfluxTarget implements ITarget {
    protected final InfluxDB influxDB;
    protected final String measurementName;
    protected final Granularity granularity;
    protected boolean errorsOccurred;

    public AbstractInfluxTarget(ConfigFile config, boolean recreate) throws IOException{
        this.measurementName = config.getInfluxTable();
        this.granularity = config.getGeneratorGranularity();
        this.influxDB = InfluxHelper.openConnection(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword());

        if(recreate){
            InfluxHelper.dropTable(influxDB, config.getInfluxTable());
            // InfluxDB creates its table (measurement) just inserting into it. We cant create it explicitly.
        }

        influxDB.setDatabase(config.getInfluxDBName());
        influxDB.enableBatch(config.getInfluxBatchsize(), config.getInfluxFlushtime(), TimeUnit.MILLISECONDS, Executors.defaultThreadFactory(), (points, throwable) -> {
            errorsOccurred = true;
        });
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
