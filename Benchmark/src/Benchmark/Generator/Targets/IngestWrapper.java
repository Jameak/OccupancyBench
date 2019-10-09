package Benchmark.Generator.Targets;

import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.Ingest.IngestControl;

import java.io.IOException;

public class IngestWrapper implements ITarget {
    private final ITarget wrappedTarget;
    private final IngestControl ingestControl;
    private boolean stop;

    public IngestWrapper(ITarget wrappedTarget, IngestControl ingestControl){
        this.wrappedTarget = wrappedTarget;
        this.ingestControl = ingestControl;
    }

    public void setStop(){
        stop = true;
    }

    @Override
    public void add(DataGenerator.GeneratedEntry entry) throws IOException {
        wrappedTarget.add(entry);
        ingestControl.add();
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
