package Benchmark.Databases.Timescale;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedColumnEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

/**
 * Writes the added entries to TimescaleDB with millisecond precision, in column-format.
 */
public class TimescaleColumnTarget extends AbstractTimescaleTarget {
    private final int batchSize;
    private int inserts = 0;
    private final GeneratedAccessPoint[] allAPs;

    public TimescaleColumnTarget(ConfigFile config, boolean recreate, GeneratedAccessPoint[] allAPs) throws SQLException {
        super(config);
        this.allAPs = allAPs;
        this.batchSize = config.getTimescaleBatchSize();

        if(recreate){
            TimescaleHelper.dropTable(connection, config.getTimescaleTable());
            TimescaleHelper.createTableWithColumnSchema(connection, config.getTimescaleTable(), allAPs);
        }

        StringBuilder sb = new StringBuilder(String.format("INSERT INTO %s (time", config.getTimescaleTable()));
        for(GeneratedAccessPoint AP : allAPs){
            sb.append(",\"");
            //AP-names contain a '-' character that needs to be escaped
            sb.append(AP.getAPname());
            sb.append("\"");
        }
        sb.append(") VALUES (?"); // First ? is for the timestamp
        for (int i = 0; i < allAPs.length; i++) {
            sb.append(",?");
        }
        sb.append(")");
        stmt = connection.prepareStatement(sb.toString());
    }

    @Override
    public void add(IGeneratedEntry entry) throws SQLException {
        assert entry instanceof GeneratedColumnEntry : "Generated entry passed to column target must be a column-entry";
        GeneratedColumnEntry columnEntry = (GeneratedColumnEntry) entry;

        Timestamp timestamp = new Timestamp(padTime(columnEntry));
        stmt.setTimestamp(1, timestamp);
        int nextParamIndex = 2;
        HashMap<String, Integer> map = columnEntry.getMapping();
        for(GeneratedAccessPoint AP : allAPs){
            stmt.setInt(nextParamIndex, map.getOrDefault(AP.getAPname(), 0));
            nextParamIndex++;
        }

        stmt.addBatch();
        inserts++;

        if(inserts == batchSize){
            inserts = 0;
            int[] counts = stmt.executeBatch();
            noErrors(counts);
        }
    }
}
