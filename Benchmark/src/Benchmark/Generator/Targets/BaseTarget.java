package Benchmark.Generator.Targets;

import Benchmark.Generator.DataGenerator;

import java.io.IOException;

public class BaseTarget implements ITarget {
    @Override
    public void add(DataGenerator.GeneratedEntry entry) throws IOException { }
    @Override
    public void close() throws Exception { }
}
