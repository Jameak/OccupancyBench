package Benchmark.Generator;

import java.util.Map;

public class Floor {
    private final int floorNumber;
    private final Map<AccessPoint, AccessPoint[]> adjacency;
    private final AccessPoint[] APs;

    public Floor(int floorNumber, Map<AccessPoint, AccessPoint[]> adjacency) {
        this.floorNumber = floorNumber;
        this.adjacency = adjacency;
        APs = adjacency.keySet().toArray(new AccessPoint[0]);
    }

    public Map<AccessPoint, AccessPoint[]> getAdjacency() {
        return adjacency;
    }

    public int getFloorNumber() {
        return floorNumber;
    }

    public AccessPoint[] getAPs() {
        return APs;
    }

    public void addNewAdjacency(AccessPoint existingAP, AccessPoint APtoAdd){
        assert adjacency.containsKey(existingAP);
        assert adjacency.get(existingAP) != null;

        AccessPoint[] existingAdj = adjacency.get(existingAP);
        AccessPoint[] newAdj = new AccessPoint[existingAdj.length+1];
        System.arraycopy(existingAdj, 0, newAdj, 0, existingAdj.length);
        newAdj[existingAdj.length] = APtoAdd;
        adjacency.put(existingAP, newAdj);
    }
}
