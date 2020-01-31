package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Ingest.IngestControl;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Wraps the given target in an ingest-controller to facilitate monitoring and control of the ingest-process.
 */
public class IngestWrapper implements ITarget {
    private final ITarget wrappedTarget;
    private final IngestControl ingestControl;
    private volatile boolean stop;

    public IngestWrapper(ITarget wrappedTarget, IngestControl ingestControl){
        this.wrappedTarget = wrappedTarget;
        this.ingestControl = ingestControl;
    }

    public void setStop(){
        stop = true;
    }

    public boolean didWrappedTargetCauseStop(){
        return wrappedTarget.shouldStopEarly();
    }

    @Override
    public void add(IGeneratedEntry entry) throws IOException, SQLException {
        wrappedTarget.add(entry);
        ingestControl.add(entry);
    }

    @Override
    public boolean shouldStopEarly() {
        return stop || wrappedTarget.shouldStopEarly();
    }

    @Override
    public void close() throws Exception {
        wrappedTarget.close();
    }
}
