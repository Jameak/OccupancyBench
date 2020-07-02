package Benchmark.Databases;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.Influx.InfluxColumnQueries;
import Benchmark.Databases.Influx.InfluxRowQueries;
import Benchmark.Databases.Kudu.KuduColumnQueries;
import Benchmark.Databases.Kudu.KuduRowQueries;
import Benchmark.Databases.Timescale.TimescaleColumnQueries;
import Benchmark.Databases.Timescale.TimescaleRowQueries;
import Benchmark.Queries.IQueries;

public class DatabaseQueriesFactory {
    /**
     * Creates a new instance of an IQueries-implementation for the database that's selected in the config file.
     * An exception will be thrown if the config-file specifies the CSV-target, since it has no IQueries implementation.
     *
     * @param config The config-file of the benchmark.
     * @return A new instance of the IQueries-implementation for the specified database.
     */
    public static IQueries createQueriesInstance(ConfigFile config){
        switch (config.getSchema()){
            case NARROW:
                switch (config.getQueriesTarget()){
                    case INFLUX:
                        return new InfluxRowQueries();
                    case TIMESCALE:
                        return new TimescaleRowQueries();
                    case KUDU:
                        return new KuduRowQueries();
                    case CSV:
                        throw new IllegalStateException("The CSV target is only for writing generated data to disk for later loading into databases. Querying the csv files isn't implemented.");
                    default:
                        throw new IllegalStateException("Unknown query target: " + config.getQueriesTarget());
                }
            case WIDE:
                switch (config.getQueriesTarget()){
                    case INFLUX:
                        return new InfluxColumnQueries();
                    case TIMESCALE:
                        return new TimescaleColumnQueries();
                    case KUDU:
                        return new KuduColumnQueries();
                    case CSV:
                        throw new IllegalStateException("The CSV target is only for writing generated data to disk for later loading into databases. Querying the csv files isn't implemented.");
                    default:
                        throw new IllegalStateException("Unknown query target: " + config.getQueriesTarget());
                }
            default:
                throw new IllegalStateException("Unknown schema: " + config.getSchema());
        }
    }
}
