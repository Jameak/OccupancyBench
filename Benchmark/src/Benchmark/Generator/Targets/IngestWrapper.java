package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.GeneratedEntry;
import Benchmark.Generator.Ingest.IngestControl;

import java.io.IOException;

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

    @Override
    public void add(GeneratedEntry entry) throws IOException {
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
