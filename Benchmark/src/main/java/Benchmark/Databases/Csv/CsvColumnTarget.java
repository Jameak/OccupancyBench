package Benchmark.Databases.Csv;

import Benchmark.Config.ConfigFile;
import Benchmark.Config.Granularity;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedColumnEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * Writes the added entries to the specified file in column-format, separated by a newline.
 */
public class CsvColumnTarget implements ITarget {
    private final AccessPoint[] allAPs;
    private final BufferedWriter writer;
    private final Granularity granularity;

    public CsvColumnTarget(ConfigFile config, AccessPoint[] allAPs) throws IOException {
        this.allAPs = allAPs;
        writer = new BufferedWriter(new FileWriter(config.getGeneratorDiskTarget()));
        this.granularity = config.getGeneratorGranularity();

        // Write a header for the CSV file:
        StringBuilder sb = new StringBuilder();
        sb.append("time");

        for(AccessPoint AP : allAPs){
            sb.append(";");
            sb.append(AP.getAPname());
        }
        writer.write(sb.toString());
        writer.write("\n");
    }

    @Override
    public void add(IGeneratedEntry entry) throws IOException {
        assert entry instanceof GeneratedColumnEntry : "Generated entry passed to column target must be a column-entry";
        GeneratedColumnEntry columnEntry = (GeneratedColumnEntry) entry;

        HashMap<String, Integer> map = columnEntry.getMapping();
        StringBuilder sb = new StringBuilder();
        sb.append(columnEntry.getTime(granularity));

        for(AccessPoint AP : allAPs){
            sb.append(";");
            sb.append(map.getOrDefault(AP.getAPname(), 0));
        }

        writer.write(sb.toString());
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
