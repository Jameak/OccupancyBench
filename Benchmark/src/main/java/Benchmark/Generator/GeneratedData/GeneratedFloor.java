package Benchmark.Generator.GeneratedData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeneratedFloor implements Serializable {
    private final int floorNumber;
    private final GeneratedAccessPoint[] APs;

    public GeneratedFloor(int floorNumber, GeneratedAccessPoint[] APsOnFloor) {
        this.floorNumber = floorNumber;
        this.APs = APsOnFloor;
    }

    public int getFloorNumber() {
        return floorNumber;
    }

    public GeneratedAccessPoint[] getAPs() {
        return APs;
    }

    public static GeneratedAccessPoint[] allAPsOnFloors(GeneratedFloor[] generatedFloors){
        List<GeneratedAccessPoint> APs = new ArrayList<>();
        for(GeneratedFloor floor : generatedFloors){
            APs.addAll(Arrays.asList(floor.getAPs()));
        }
        return APs.toArray(new GeneratedAccessPoint[0]);
    }
}
