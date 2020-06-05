package Benchmark.SeedLoader;

import Benchmark.SeedLoader.Metadata.FloorMetadata;
import Benchmark.SeedLoader.Seeddata.SeedEntries;

public class SeedData {
    public final FloorMetadata[] floorMetadata;
    public final SeedEntries seedEntries;

    public SeedData(FloorMetadata[] floorMetadata, SeedEntries seedEntries){
        this.floorMetadata = floorMetadata;
        this.seedEntries = seedEntries;
    }
}
