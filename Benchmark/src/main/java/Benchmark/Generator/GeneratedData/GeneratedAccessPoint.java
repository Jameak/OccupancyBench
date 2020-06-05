package Benchmark.Generator.GeneratedData;

import java.io.Serializable;

public class GeneratedAccessPoint implements Serializable {
    private final String APname;
    private final String originalName;

    public GeneratedAccessPoint(String APname, String originalName) {
        this.APname = APname;
        this.originalName = originalName;
    }

    public String getAPname() {
        return APname;
    }

    public String getOriginalName() {
        return originalName;
    }
}
