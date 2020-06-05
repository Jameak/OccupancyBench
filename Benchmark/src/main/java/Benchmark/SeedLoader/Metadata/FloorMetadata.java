package Benchmark.SeedLoader.Metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FloorMetadata {
    public final String floorKey;
    public final boolean eligibleForAutoScaling;
    public final List<String> accessPointsOnFloor;
    public final Map<String,String> combinedAccessPoints = new HashMap<>();

    public FloorMetadata(String floorKey, boolean eligibleForAutoScaling, List<String> accessPointsOnFloor) {
        this.floorKey = floorKey;
        this.eligibleForAutoScaling = eligibleForAutoScaling;
        this.accessPointsOnFloor = accessPointsOnFloor;
    }
}
