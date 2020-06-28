package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.IGeneratedEntry;

import java.io.IOException;
import java.sql.SQLException;

/**
 * A shared interface for all generation-targets.
 *
 * Since a target might hold handles/resources that need to be released, they must also be AutoCloseable.
 */
public interface ITarget extends AutoCloseable {
    /**
     * Add the given entry (row) to the database.
     * Entries may be batched if desired.
     *
     * If entries are batched, make sure that an incomplete
     * batch is submitted before the handle is closed.
     */
    void add(IGeneratedEntry entry) throws IOException, SQLException;

    /**
     * Indicate that we should gracefully abort because errors occurred during entry insertion.
     */
    boolean shouldStopEarly();
}
