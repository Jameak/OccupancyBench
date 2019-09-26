package Benchmark.Generator.Targets;

import Benchmark.Generator.DataGenerator;

import java.io.IOException;

public interface ITarget extends AutoCloseable {
    void add(DataGenerator.GeneratedEntry entry) throws IOException;
}
