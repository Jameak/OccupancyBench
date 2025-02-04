package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.IGeneratedEntry;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Facilitates composition of ITarget instances.
 */
public class MultiTarget implements ITarget {
    private final ITarget target1;
    private final ITarget target2;

    public MultiTarget(ITarget target1, ITarget target2){
        this.target1 = target1;
        this.target2 = target2;
    }

    @Override
    public void add(IGeneratedEntry entry) throws IOException, SQLException {
        target1.add(entry);
        target2.add(entry);
    }

    @Override
    public boolean shouldStopEarly() {
        return target1.shouldStopEarly() || target2.shouldStopEarly();
    }

    @Override
    public void close() throws Exception {
        target1.close();
        target2.close();
    }
}
