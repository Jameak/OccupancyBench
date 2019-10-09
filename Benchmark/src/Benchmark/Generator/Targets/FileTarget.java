package Benchmark.Generator.Targets;

import Benchmark.Generator.DataGenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

public class FileTarget implements ITarget {
    private BufferedWriter writer;

    public FileTarget(String path, String outputFileName) throws IOException {
        writer = new BufferedWriter(new FileWriter(Paths.get(path).resolve(outputFileName).toString()));
    }

    @Override
    public void add(DataGenerator.GeneratedEntry entry) throws IOException {
        writer.write(entry.toString());
        writer.write("\n");
    }

    @Override
    public boolean shouldStopEarly() {
        return false;
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
