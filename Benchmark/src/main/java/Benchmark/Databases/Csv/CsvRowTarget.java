package Benchmark.Databases.Csv;

import Benchmark.Config.ConfigFile;
import Benchmark.Config.Granularity;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes the added entries to the specified file in row-format, separated by a newline.
 */
public class CsvRowTarget implements ITarget {
    private final BufferedWriter writer;
    private final Granularity granularity;

    public CsvRowTarget(ConfigFile config) throws IOException {
        writer = new BufferedWriter(new FileWriter(config.getGeneratorDiskTarget()));
        this.granularity = config.getGeneratorGranularity();
    }

    @Override
    public void add(IGeneratedEntry entry) throws IOException {
        assert entry instanceof GeneratedRowEntry : "Generated entry passed to row target must be a row-entry";
        GeneratedRowEntry rowEntry = (GeneratedRowEntry) entry;

        writer.write(rowEntry.toString(granularity));
        writer.write("\n");
    }

    @Override
    public boolean shouldStopEarly() {
        return false;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
