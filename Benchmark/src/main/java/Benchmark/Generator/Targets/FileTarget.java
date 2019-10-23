package Benchmark.Generator.Targets;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedEntry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes the added entries to the specified file, separated by a newline.
 */
public class FileTarget implements ITarget {
    private final BufferedWriter writer;
    private final ConfigFile.Granularity granularity;

    public FileTarget(String outputPath, ConfigFile.Granularity granularity) throws IOException {
        writer = new BufferedWriter(new FileWriter(outputPath));
        this.granularity = granularity;
    }

    @Override
    public void add(GeneratedEntry entry) throws IOException {
        writer.write(entry.toString(granularity));
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
