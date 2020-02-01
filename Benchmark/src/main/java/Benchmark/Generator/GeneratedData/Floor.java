package Benchmark.Generator.GeneratedData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Holds metadata about a specific generated floor.
 */
public class Floor implements Serializable {
    private final int floorNumber;
    private final AccessPoint[] APs;

    public Floor(int floorNumber, AccessPoint[] APsOnFloor) {
        this.floorNumber = floorNumber;
        this.APs = APsOnFloor;
    }

    public int getFloorNumber() {
        return floorNumber;
    }

    public AccessPoint[] getAPs() {
        return APs;
    }

    public static AccessPoint[] allAPsOnFloors(Floor[] generatedFloors){
        List<AccessPoint> APs = new ArrayList<>();
        for(Floor floor : generatedFloors){
            APs.addAll(Arrays.asList(floor.getAPs()));
        }
        return APs.toArray(new AccessPoint[0]);
    }
}
