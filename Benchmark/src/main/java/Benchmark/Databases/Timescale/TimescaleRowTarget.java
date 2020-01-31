package Benchmark.Databases.Timescale;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;

import java.sql.*;

/**
 * Writes the added entries to TimescaleDB with millisecond precision, in row-format.
 */
public class TimescaleRowTarget extends AbstractTimescaleTarget {
    private final int batchSize;
    private int inserts = 0;

    public TimescaleRowTarget(ConfigFile config, boolean recreate) throws SQLException {
        super(config);
        this.batchSize = config.getTimescaleBatchSize();

        if(recreate){
            TimescaleHelper.dropTable(connection, config.getTimescaleTable());
            TimescaleHelper.createTableWithRowSchema(connection, config.getTimescaleTable());
        }

        stmt = connection.prepareStatement(
                String.format("INSERT INTO %s (time, AP, clients) VALUES (?, ?, ?)", config.getTimescaleTable()));
    }

    @Override
    public void add(IGeneratedEntry entry) throws SQLException {
        assert entry instanceof GeneratedRowEntry : "Generated entry passed to row target must be a row-entry";
        GeneratedRowEntry rowEntry = (GeneratedRowEntry) entry;

        Timestamp timestamp = new Timestamp(padTime(rowEntry));
        stmt.setTimestamp(1, timestamp);
        stmt.setString(2, rowEntry.getAP());
        stmt.setInt(3, rowEntry.getNumClients());
        stmt.addBatch();
        inserts++;

        if(inserts == batchSize){
            inserts = 0;
            int[] counts = stmt.executeBatch();
            noErrors(counts);
        }
    }
}
