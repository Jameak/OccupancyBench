package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.GeneratedEntry;

import java.io.IOException;

/**
 * A shared interface for all generation-targets.
 *
 * Since a target might hold handles/resources that need to be released, they must also be AutoCloseable.
 */
public interface ITarget extends AutoCloseable {
    void add(GeneratedEntry entry) throws IOException;
    boolean shouldStopEarly();
}
