package Benchmark.Generator.Targets;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedEntry;

import java.sql.*;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Writes the added entries to TimescaleDB with millisecond precision.
 */
public class TimescaleTarget extends JdbcTarget {
    private final int batchSize;
    private final ConfigFile.Granularity granularity;
    private int inserts = 0;
    private PreparedStatement stmt;

    public TimescaleTarget(String host, String database, String username, String password, String table, boolean recreate, int batchSize, boolean enableBatchRewrite, ConfigFile.Granularity granularity) throws SQLException {
        this.batchSize = batchSize;
        // Note: Postgres doesn't seem to support nano-second timestamps, so set granularity to milliseconds if nanoseconds is set.
        //       This is mentioned in the config setting for granularity as well.
        this.granularity = granularity == ConfigFile.Granularity.NANOSECOND ? ConfigFile.Granularity.MILLISECOND : granularity;

        DriverManager.registerDriver(new org.postgresql.Driver());
        // TODO: Include SSL-parameter in the connection string?
        String db;
        if(enableBatchRewrite){
            db = String.format("jdbc:postgresql://%s/%s?reWriteBatchedInserts=true", host, database);
        } else {
            db = String.format("jdbc:postgresql://%s/%s", host, database);
        }
        connection = DriverManager.getConnection(db, username, password);

        if(recreate){
            String query1 = String.format("DROP TABLE %s", table);
            String query2 = String.format(
                    "CREATE TABLE %s (" +
                            "time    TIMESTAMP NOT NULL, " +
                            "AP      TEXT      NOT NULL," +
                            "clients INTEGER   NOT NULL)", table);
            String query3 = String.format("SELECT create_hypertable('%s', 'time')", table);

            //TODO: Create more indexes when the final queries are known?
            String query4 = String.format("CREATE INDEX ON %s (AP, time DESC)", table);

            try(Statement statement = connection.createStatement()){
                try{
                    statement.executeUpdate(query1);
                } catch (SQLException e){
                    // Table didn't exist.
                }
                statement.executeUpdate(query2);
                statement.execute(query3);
                statement.executeUpdate(query4);
            }
        }

        stmt = connection.prepareStatement(
                String.format("INSERT INTO %s (time, AP, clients) VALUES (?, ?, ?)", table));
    }

    @Override
    public void add(GeneratedEntry entry) throws SQLException {
        Timestamp timestamp = new Timestamp(padTime(entry));
        stmt.setTimestamp(1, timestamp);
        stmt.setString(2, entry.getAP());
        stmt.setInt(3, entry.getNumClients());
        stmt.addBatch();
        inserts++;

        if(inserts == batchSize){
            inserts = 0;
            int[] counts = stmt.executeBatch();
            noErrors(counts);
        }
    }

    private long padTime(GeneratedEntry entry){
        // The Timestamp-constructor expects a long of milliseconds, so we need to pad to that precision regardless of
        //   the desired granularity, so first we truncate and then pad if needed.
        long granularTime = entry.getTime(granularity);
        return granularity.toTimeUnit().convert(granularTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        stmt.executeBatch();
        stmt.close();
        connection.close();
    }

    private void noErrors(int[] counts){
        for (int count : counts) {
            // With "reWriteBatchedInserts" disabled, the return-value is the number of affected lines, which should be 1.
            // With "reWriteBatchedInserts" enabled, the return-code of a successful "executeBatch" is -2 aka SUCCESS_NO_INFO.
            if (!(count == -2 || count == 1)) {
                error = true;
                assert false : "Error in timescale batch insert. Count was: " + count;
            }
        }
    }
}
