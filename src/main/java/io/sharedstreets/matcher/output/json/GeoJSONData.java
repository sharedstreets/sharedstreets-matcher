package io.sharedstreets.matcher.output.json;


import java.io.IOException;

public abstract class GeoJSONData {

    public String id;
    public String type;

    public abstract String toGeoJSON() throws IOException;
}
