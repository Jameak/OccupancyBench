package Benchmark.Generator.GeneratedData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds metadata about a specific generated access point.
 */
public class AccessPoint implements Serializable {
    private final APLocation location;
    private final String APname;
    private final String humanReadableName; // For debugging purposes only
    private AccessPoint[] partners;
    private Integer mapID;

    public AccessPoint(APLocation location, String APname, String humanReadableName){
        this.location = location;
        this.APname = APname;
        this.humanReadableName = humanReadableName;
        mapID = -1;
    }

    public void setPartners(AccessPoint[] partners){
        this.partners = partners;
    }

    public boolean hasPartners(){
        return this.partners != null && partners.length != 0;
    }

    public APLocation getLocation() {
        return location;
    }

    public AccessPoint[] getPartners() {
        return partners;
    }

    public static void PartnerAll(AccessPoint... APs){
        for (int i = 0; i < APs.length; i++){
            List<AccessPoint> otherAPs = new ArrayList<>(APs.length);
            for(int j = 0; j < APs.length; j++){
                if(i == j) continue;
                otherAPs.add(APs[j]);
            }
            APs[i].setPartners(otherAPs.toArray(new AccessPoint[0]));
        }
    }

    public Integer getMapID() {
        return mapID;
    }

    public void setMapID(Integer mapID) {
        this.mapID = mapID;
    }

    public boolean hasMapID(){
        return mapID != -1;
    }

    public String getAPname() {
        return APname;
    }

    public enum APLocation {
        // Normal rooms
        Combined, Large, Small, MeetingRoom, MeetingBox, Wing,
        // Specials
        Auditorium, // Considered combined
        Cantina,    // Considered combined
        Bar,        // Considered combined
        Coffeebar,
        Outdoors,
        Atrium,
        Lab
    }
}
