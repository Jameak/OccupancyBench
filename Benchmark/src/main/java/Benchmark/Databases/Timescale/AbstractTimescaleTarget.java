package Benchmark.Databases.Timescale;

import Benchmark.Config.ConfigFile;
import Benchmark.Config.Granularity;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for Influx target-implementations containing default-implementation for some ITarget-functions,
 * convenience functions and a constructor that handles the database-setup that any Timescale-implementation must do.
 */
public abstract class AbstractTimescaleTarget implements ITarget {
    protected final Granularity granularity;
    protected PreparedStatement stmt;
    protected Connection connection;
    protected boolean error;

    public AbstractTimescaleTarget(ConfigFile config) throws SQLException {
        // NOTE: Postgres doesn't support nano-second timestamps, so set granularity to milliseconds if nanoseconds is set.
        //       This is mentioned in the config documentation for granularity as well.
        this.granularity = config.getGeneratorGranularity() == Granularity.NANOSECOND || config.getGeneratorGranularity() == Granularity.MICROSECOND
                ? Granularity.MILLISECOND : config.getGeneratorGranularity();

        this.connection = TimescaleHelper.openConnection(config.getTimescaleUsername(), config.getTimescalePassword(),
                config.getTimescaleHost(), config.getTimescaleDBName(), config.reWriteBatchedTimescaleInserts());
    }

    protected long padTime(IGeneratedEntry entry){
        // The Timestamp-constructor expects a long of milliseconds, so we need to pad to that precision regardless of
        //   the desired granularity, so first we truncate and then pad if needed.
        long granularTime = entry.getTime(granularity);
        return TimeUnit.MILLISECONDS.convert(granularTime, granularity.toTimeUnit());
    }

    @Override
    public void close() throws Exception {
        stmt.executeBatch();
        stmt.close();
        connection.close();
    }

    @Override
    public boolean shouldStopEarly(){
        return error;
    }

    protected void checkForErrors(int[] counts){
        for (int count : counts) {
            // With "reWriteBatchedInserts" disabled, the return-value is the number of affected lines, which should be 1.
            // With "reWriteBatchedInserts" enabled, the return-code of a successful "executeBatch" is -2 aka SUCCESS_NO_INFO.
            if (!(count == -2 || count == 1)) {
                error = true;
                Logger.LOG("TIMESCALE: Error during timescale batch insert. Count was " + count);
            }
        }
    }
}
