package Benchmark.SeedLoader;

import Benchmark.Config.ConfigFile;
import Benchmark.SeedLoader.Metadata.FloorMetadata;
import Benchmark.SeedLoader.Metadata.MetadataLoader;
import Benchmark.SeedLoader.Seeddata.SeedEntries;
import Benchmark.SeedLoader.Seeddata.SeeddataLoader;
import Benchmark.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoaderFacade {
    public static SeedData LoadSeedData(ConfigFile config) throws IOException{
        String idmapFile = config.getGeneratorInputIdmap();
        String floorFile = config.getGeneratorInputFloorInfoFile();
        String floorMapFile = config.getGeneratorInputFloorMapFile();
        String ignoreFile = config.getGeneratorInputIgnoreFile();
        String combinedFile = config.getGeneratorInputCombinedFile();
        String seedDataFolder = config.getGeneratorInputPropabilityFolder();
        String separator = config.getGeneratorInputSeparator();

        try {
            List<MetadataLoader.FloorData> floorData = MetadataLoader.LoadFloorFile(floorFile, separator);
            Map<String, List<String>> floorMap = MetadataLoader.LoadFloorToAccessPointFile(floorMapFile, separator);
            Set<String> ignoreData = MetadataLoader.LoadIgnoreFile(ignoreFile);
            List<Set<String>> combinedData = MetadataLoader.LoadCombinedFile(combinedFile, separator);
            //TODO: Add a check for whether to load the idmap at all, Requiring the file to be present but allowing it
            //      to be empty is stupid...
            Map<String, String> idMapData = MetadataLoader.LoadIdMap(idmapFile, separator);

            FloorMetadata[] floorMetadata = MetadataLoader.CombineMetadata(floorData, floorMap, ignoreData, combinedData);
            SeedEntries seedData;
            if(idMapData.isEmpty()){
                seedData = SeeddataLoader.LoadSeedData(separator, seedDataFolder, ignoreData, floorMetadata);
            } else {
                seedData = SeeddataLoader.LoadSeedData(separator, seedDataFolder, ignoreData, floorMetadata, idMapData);
            }
            return new SeedData(floorMetadata, seedData);
        } catch (IOException e) {
            Logger.LOG("IO Error during seed loading.");
            throw e;
        }
    }
}
