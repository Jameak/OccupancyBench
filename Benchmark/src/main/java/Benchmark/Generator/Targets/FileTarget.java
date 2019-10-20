package Benchmark.Generator.Targets;

import Benchmark.Generator.GeneratedData.GeneratedEntry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes the added entries to the specified file, separated by a newline.
 */
public class FileTarget implements ITarget {
    private BufferedWriter writer;

    public FileTarget(String outputPath) throws IOException {
        writer = new BufferedWriter(new FileWriter(outputPath));
    }

    @Override
    public void add(GeneratedEntry entry) throws IOException {
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
