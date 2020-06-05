package Benchmark.Databases;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.Csv.CsvColumnTarget;
import Benchmark.Databases.Csv.CsvRowTarget;
import Benchmark.Databases.Influx.InfluxColumnTarget;
import Benchmark.Databases.Influx.InfluxRowTarget;
import Benchmark.Databases.Kudu.KuduColumnTarget;
import Benchmark.Databases.Kudu.KuduRowTarget;
import Benchmark.Databases.Timescale.TimescaleColumnTarget;
import Benchmark.Databases.Timescale.TimescaleRowTarget;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.Targets.ITarget;

import java.io.IOException;
import java.sql.SQLException;

public class DatabaseTargetFactory {
    /**
     * Creates a new instance of an ITarget-implementation for the specified database.
     *
     * @param target The database whose target-implementation to instantiate.
     * @param config The config-file of the benchmark.
     * @param recreate An indicator for whether the database implementation should delete all previous entries and recreate itself on instantiation.
     * @param allAPs An array of all the access points to be stored in the database.
     * @return A new instance of the ITarget-implementation for the specified database.
     */
    public static ITarget createDatabaseTarget(DBTargets target, ConfigFile config, boolean recreate, GeneratedAccessPoint[] allAPs) throws IOException, SQLException {
        switch (config.getSchema()){
            case ROW:
                switch (target){
                    case INFLUX:
                        return new InfluxRowTarget(config, recreate);
                    case CSV:
                        return new CsvRowTarget(config);
                    case TIMESCALE:
                        return new TimescaleRowTarget(config, recreate);
                    case KUDU:
                        return new KuduRowTarget(config, recreate);
                }

                throw new IllegalStateException("Target " + target + " with schema " + config.getSchema() + " has no supported target-implementations.");
            case COLUMN:
                assert allAPs.length > 0 : "Column-schema must know the APs up front.";

                switch (target){
                    case CSV:
                        return new CsvColumnTarget(config, allAPs);
                    case INFLUX:
                        return new InfluxColumnTarget(config, recreate, allAPs);
                    case TIMESCALE:
                        return new TimescaleColumnTarget(config, recreate, allAPs);
                    case KUDU:
                        return new KuduColumnTarget(config, recreate, allAPs);
                }

                throw new IllegalStateException("Target " + target + " with schema " + config.getSchema() + " has no supported target-implementations.");
            default:
                throw new IllegalStateException("Unknown schema: " + config.getSchema());
        }
    }
}
