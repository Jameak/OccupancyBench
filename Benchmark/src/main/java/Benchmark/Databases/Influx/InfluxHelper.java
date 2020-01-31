package Benchmark.Databases.Influx;

import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Contains static helper functions used by the Influx-database implementations.
 */
public class InfluxHelper {
    /**
     * Opens a connection to the Influx database.
     * @param url The url of the influx-database to connect to. Must have the protocol specified (e.g. starts with "http://")
     */
    public static InfluxDB openConnection(String url, String username, String password) throws IOException {
        // Some queries take too long to execute on my dev-laptop with the default timeout.
        // If a query times out an exception is thrown which stops the queries, so increase the timeout here.
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder().readTimeout(60, TimeUnit.SECONDS);
        InfluxDB db = InfluxDBFactory.connect(url, username, password, httpClientBuilder);

        if(db.ping().getVersion().equalsIgnoreCase("unknown")) {
            db.close();
            throw new IOException("No connection to Influx database.");
        }

        return db;
    }

    /**
     * Drops the specified database.
     */
    public static void dropDatabase(InfluxDB db, String dbName){
        db.query(new Query("DROP DATABASE " + dbName));
    }

    /**
     * Creates the specified database.
     */
    public static void createDatabase(InfluxDB db, String dbName){
        db.query(new Query("CREATE DATABASE " + dbName));
    }
}
