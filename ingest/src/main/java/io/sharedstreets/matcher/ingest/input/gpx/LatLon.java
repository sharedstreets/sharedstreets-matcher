package io.sharedstreets.matcher.ingest.input.gpx;


public class LatLon {

    private final double lat;

    private final double lon;

    private final long time;

    private final Double speed;

    public LatLon(final double lat, final double lon, final long time, final double speed) {
        this.lat = lat;
        this.lon = lon;
        this.time = time;
        this.speed = speed;
        this.name = null;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public long getTime() {
        return time;
    }

    public Double getSpeed() {
        return speed;
    }

    private final String name;

    public LatLon(final double lat, final double lon, final long time, final String name) {
        this.lat = lat;
        this.lon = lon;
        this.time = time;
        this.speed = null;
        this.name = name;
    }

    public String getName() {
        return name;
    }

}