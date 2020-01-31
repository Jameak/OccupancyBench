package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.IGeneratedEntry;

/**
 * A base-implementation of the ITarget interface that does nothing. Exists to be wrapped and to avoid
 * passing in NULL. Instead of NULL, pass in this class to avoid targeting anything.
 */
public class BaseTarget implements ITarget {
    @Override
    public void add(IGeneratedEntry entry) { }

    @Override
    public boolean shouldStopEarly() { return false; }

    @Override
    public void close() { }
}
