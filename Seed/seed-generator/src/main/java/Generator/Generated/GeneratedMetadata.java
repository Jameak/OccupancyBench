package Generator.Generated;

import java.util.List;

public class GeneratedMetadata {
    public final List<GeneratedFloor> floorData;
    public final List<GeneratedAccessPoint> accessPoints;
    public final List<GeneratedCombination> combinations;
    public final List<GeneratedAccessPoint> ignoredAccessPoints;

    public GeneratedMetadata(List<GeneratedFloor> floorData, List<GeneratedAccessPoint> accessPoints, List<GeneratedCombination> combinations, List<GeneratedAccessPoint> ignoredAccessPoints) {
        this.floorData = floorData;
        this.accessPoints = accessPoints;
        this.combinations = combinations;
        this.ignoredAccessPoints = ignoredAccessPoints;
    }
}
